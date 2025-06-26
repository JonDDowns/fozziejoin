// All text distance algorithms either directly use or import the
// the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::utils::robj_index_map;
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::iter::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use textdistance::str::{hamming, levenshtein};

pub mod damerau_levenshtein;
pub mod osa;

// Define a trait for string distance calculations
pub trait EditDistance: Send + Sync {
    /// Compute the raw distance score between two input strings using a fuzzy matching algorithm.
    ///
    /// This method performs a pairwise comparison between `s1` and `s2`
    /// and returns a distance score as a non-negative integer. The specific
    /// definition of "distance" (e.g., edit distance, Hamming distance, LCS length)
    /// depends on the algorithm implemented by the struct that defines this method.
    ///
    /// # Parameters
    ///
    /// - `s1`: The first input string.
    /// - `s2`: The second input string.
    ///
    /// # Returns
    ///
    /// A `usize` representing the computed distance between `s1` and `s2`.
    /// Lower scores generally indicate higher similarity, though this interpretation
    /// depends on the specific algorithm.
    ///
    /// # Notes
    ///
    /// - This method does not normalize or scale the result—it returns the raw score.
    /// - Behavior and return values should be consistent with the algorithm semantics.
    ///
    fn compute(&self, s1: &str, s2: &str) -> usize;

    fn compute_char(&self, _s1: &Vec<char>, _s2: &Vec<char>) -> usize {
        0
    }

    /// Perform approximate matching between two indexed string maps using a fuzzy distance metric.
    ///
    /// This method compares keys in `map1` against keys in `map2`, applying a fuzzy string
    /// similarity algorithm (implemented by the caller via `self.word_map_lookup_and_compare`)
    /// to generate matched index pairs whose distance is within `max_distance`. It filters
    /// comparisons based on string length proximity to improve efficiency.
    ///
    /// # Parameters
    ///
    /// - `map1`: A map from unique string tokens to their corresponding row indices (e.g., from `df1`).
    /// - `map2`: A second map of tokens to row indices (e.g., from `df2`), to match against.
    /// - `max_distance`: The maximum allowable string distance for a match to be considered valid.
    /// - `full`: If `true`, includes non-overlapping pairs in the join (used for `"full"` joins).
    /// - `nthread`: Optional number of threads to use for parallel distance calculation.
    ///
    /// # Returns
    ///
    /// A vector of matched index pairs in the form:
    /// - `(left_idx, right_idx, distance)` where
    ///   - `left_idx` refers to a row in `map1`
    ///   - `right_idx` refers to a row in `map2`
    ///   - `distance` is the computed similarity or distance (may be `None` for exact matches)
    ///
    /// # Implementation Notes
    ///
    /// - Uses string length filtering to prune unnecessary comparisons across maps.
    /// - Matches are computed in parallel via `rayon::par_iter`.
    /// - Requires the implementor to define `word_map_lookup_and_compare`, which encapsulates
    ///   the specific distance logic and output structure.
    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
        let map1 = robj_index_map(left, left_key);
        let map2 = robj_index_map(right, right_key);

        // If user specified a number of threads, build a custom pool
        if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
        };

        // We don't need to check any strings where lengths differ by more than max
        // For RHS, keep a map of lengths of all strings
        // We use this later to subset the columns we compare in each set
        let mut length_map: HashMap<usize, Vec<&str>> = HashMap::new();
        for key in map2.keys() {
            let key_len = key.len();
            length_map.entry(key_len).or_insert(Vec::new()).push(key);
        }

        // Get the min/max string lengths in the RHS dataset
        let min_key = length_map
            .keys()
            .min()
            .expect("Problem extracting minimum key length RHS");
        let max_key = length_map
            .keys()
            .max()
            .expect("Problem extracting maximum key length for RHS");

        // Begin generation of all matched indices
        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                self.word_map_lookup_and_compare(
                    k1,
                    v1,
                    &length_map,
                    &map2,
                    &full,
                    &max_distance,
                    min_key,
                    max_key,
                )
            })
            .flatten()
            .collect();
        idxs
    }

    fn word_map_lookup_and_compare(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        length_map: &HashMap<usize, Vec<&str>>,
        idx_map: &HashMap<&str, Vec<usize>>,
        full: &bool,
        max_distance: &f64,
        min_key: &usize,
        max_key: &usize,
    ) -> Option<Vec<(usize, usize, Option<f64>)>> {
        // Skip all comparisons if string is NA
        if !full {
            if k1.is_na() {
                return None;
            }
        }

        // Get range of lengths within max distance of current
        let k1_len = k1.len();
        let start_len = match full {
            true => *min_key,
            false => k1_len.saturating_sub(*max_distance as usize),
        };
        let end_len = match full {
            true => *max_key + 1,
            false => k1_len.saturating_add(*max_distance as usize + 1),
        };

        // Start a list to collect results
        let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

        // Begin making string comparisons
        for i in start_len..end_len {
            if let Some(lookup) = length_map.get(&i) {
                lookup.iter().for_each(|k2| {
                    // Skip this iter if RHS is NA
                    if k2.is_na() && *full {
                        let v2 = idx_map.get(k2).unwrap();
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, NA_REAL));
                        });
                        return;
                    }

                    // No need to run distance functions if exactly the same
                    if &k1 == k2 {
                        let v2 = idx_map.get(k2).unwrap();
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, Some(0.)));
                        });
                        return;
                    }

                    // Run distance calculation
                    let dist = self.compute(&k1, &k2) as f64;

                    // Check vs. threshold
                    if dist <= *max_distance || *full {
                        let v2 = idx_map.get(k2).unwrap();
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, Some(dist)));
                        });
                        return;
                    }
                });
            }
        }

        // Return all matches, if any
        if idxs.is_empty() {
            return None;
        } else {
            return Some(idxs);
        }
    }
}

pub struct Levenshtein;
impl EditDistance for Levenshtein {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        levenshtein(s1, s2)
    }
}

pub struct Hamming;
impl EditDistance for Hamming {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        // Change default TD behavior: unequal strings sizes should auto-fail
        if s1.chars().count() != s2.chars().count() {
            return usize::MAX;
        }

        // Otherwise, calculate
        hamming(s1, s2)
    }
}

pub struct LCSStr;
impl EditDistance for LCSStr {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        let m = s1.len();
        let n = s2.len();
        let mut dp = vec![vec![0; n + 1]; m + 1];

        for (i, c1) in s1.chars().enumerate() {
            for (j, c2) in s2.chars().enumerate() {
                if c1 == c2 {
                    dp[i + 1][j + 1] = dp[i][j] + 1;
                } else {
                    dp[i + 1][j + 1] = dp[i + 1][j].max(dp[i][j + 1]);
                }
            }
        }

        (m + n) - 2 * dp[m][n]
    }
}
