use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::iter::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use textdistance::str::{damerau_levenshtein, hamming, levenshtein};

// Define a trait for string distance calculations
pub trait EditDistance: Send + Sync {
    fn compute(&self, s1: &str, s2: &str) -> usize;

    #[cfg(not(target_os = "windows"))]
    fn fuzzy_indices(
        &self,
        map1: HashMap<&str, Vec<usize>>,
        map2: HashMap<&str, Vec<usize>>,
        max_distance: f64,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
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
        let min_key = length_map.keys().min().expect("Problem?!");
        let max_key = length_map.keys().max().expect("Problem?!");

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

    #[cfg(target_os = "windows")]
    fn fuzzy_indices(
        &self,
        map1: HashMap<&str, Vec<usize>>,
        map2: HashMap<&str, Vec<usize>>,
        max_distance: f64,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
        let nt = if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
            nt
        } else {
            rayon::current_num_threads()
        };

        // We don't need to check any strings where lengths differ by more than max
        // For RHS, keep a map of lengths of all strings
        // We use this later to subset the columns we compare in each set
        let mut length_map: HashMap<usize, Vec<&str>> = HashMap::new();
        for key in map2.keys() {
            let key_len = key.len();
            length_map.entry(key_len).or_insert(Vec::new()).push(key);
        }
        let min_key = length_map.keys().min().expect("Problem?!");
        let max_key = length_map.keys().max().expect("Problem?!");

        let batch_size = map1.len().div_ceil(nt);

        // Begin generation of all matched indices
        let hm: Vec<(&str, &Vec<usize>)> = map1.iter().map(|(a, b)| (*a, b)).collect();
        let idxs: Vec<(usize, usize, Option<f64>)> = hm
            .par_chunks(batch_size)
            .flat_map_iter(|chunk| {
                chunk.iter().filter_map(|(k1, v1)| {
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
                    if (k2.is_na() || k1.is_na()) && *full {
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
        // Change default TD behavior: unequal strings sizes
        // should auto-fail
        if s1.chars().count() != s2.chars().count() {
            return usize::MAX;
        }

        // Otherwise, calculate
        hamming(s1, s2)
    }
}

pub struct DamerauLevenshtein;
impl EditDistance for DamerauLevenshtein {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        damerau_levenshtein(s1, s2)
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

pub struct OSA;
impl EditDistance for OSA {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        //damerau_levenshtein_restricted(s1, s2)
        let m = s1.len();
        let n = s2.len();
        let mut dp = vec![vec![0; n + 1]; m + 1];

        // Initialize base cases
        for i in 0..=m {
            dp[i][0] = i;
        }
        for j in 0..=n {
            dp[0][j] = j;
        }

        // Compute OSA distance using DP
        for i in 1..=m {
            for j in 1..=n {
                let cost = if s1.chars().nth(i - 1) == s2.chars().nth(j - 1) {
                    0
                } else {
                    1
                };

                dp[i][j] = *[
                    dp[i - 1][j] + 1,        // Deletion
                    dp[i][j - 1] + 1,        // Insertion
                    dp[i - 1][j - 1] + cost, // Substitution
                ]
                .iter()
                .min()
                .unwrap();

                // Handle transpositions
                if i > 1
                    && j > 1
                    && s1.chars().nth(i - 1) == s2.chars().nth(j - 2)
                    && s1.chars().nth(i - 2) == s2.chars().nth(j - 1)
                {
                    dp[i][j] = dp[i][j].min(dp[i - 2][j - 2] + cost);
                }
            }
        }

        dp[m][n] // Final OSA distance
    }
}
