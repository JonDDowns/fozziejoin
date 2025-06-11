use crate::utils::sort_unzip_triplet;
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::iter::*;
use std::collections::HashMap;
use textdistance::str::{
    damerau_levenshtein, damerau_levenshtein_restricted, hamming, lcsstr, levenshtein,
};

// Define a trait for string distance calculations
pub trait EditDistance: Send + Sync {
    fn compute(&self, s1: &str, s2: &str) -> usize;

    fn fuzzy_indices(
        &self,
        map1: HashMap<&str, Vec<usize>>,
        map2: HashMap<&str, Vec<usize>>,
        max_distance: f64,
    ) -> (Vec<usize>, Vec<usize>, Vec<Option<f64>>) {
        let md = max_distance as usize;
        // We don't need to check any strings where lengths differ by more than max
        // For RHS, keep a map of lengths of all strings
        // We use this later to subset the columns we compare in each set
        let mut length_hm: HashMap<usize, Vec<&str>> = HashMap::new();
        for key in map2.keys() {
            let key_len = key.len();
            length_hm.entry(key_len).or_insert(Vec::new()).push(key);
        }

        // Begin generation of all matched indices
        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                // Skip all comparisons if string is NA
                if k1.is_na() {
                    return None;
                }

                // Get range of lengths within max distance of current
                let k1_len = k1.len();
                let start_len = k1_len.saturating_sub(md);
                let end_len = k1_len + md + 1;

                // Start a list to collect results
                let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

                // Begin making string comparisons
                for i in start_len..end_len {
                    if let Some(lookup) = length_hm.get(&i) {
                        lookup.iter().for_each(|k2| {
                            // Skip this iter if RHS is NA
                            if k2.is_na() {
                                return;
                            }
                            let v2 = map2.get(k2).unwrap();

                            // No need to run distance functions if exactly the same
                            if k1 == k2 {
                                iproduct!(v1, v2).for_each(|(v1, v2)| {
                                    idxs.push((*v1, *v2, Some(0.)));
                                });
                                return;
                            }

                            let dist = self.compute(&k1, &k2) as f64;

                            // Check vs. threshold
                            if dist <= max_distance {
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
            })
            .flatten()
            .collect();

        sort_unzip_triplet(idxs)
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
        lcsstr(s1, s2)
    }
}

pub struct OSA;
impl EditDistance for OSA {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        damerau_levenshtein_restricted(s1, s2)
    }
}
