use crate::utils::sorted_unzip;
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
        max_distance: usize,
    ) -> (Vec<usize>, Vec<usize>) {
        // We don't need to check any strings where lengths differ by more than max
        // For RHS, keep a map of lengths of all strings
        // We use this later to subset the columns we compare in each set
        let mut length_hm: HashMap<usize, Vec<&str>> = HashMap::new();
        for key in map2.keys() {
            let key_len = key.len();
            length_hm.entry(key_len).or_insert(Vec::new()).push(key);
        }

        // Begin generation of all matched indices
        let idxs: Vec<(usize, usize)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                // Get range of lengths within max distance of current
                let k1_len = k1.len();
                let start_len = k1_len.saturating_sub(max_distance);
                let end_len = k1_len + max_distance + 1;

                // Start a list to collect results
                let mut idxs: Vec<(usize, usize)> = Vec::new();

                // Begin making string comparisons
                for i in start_len..end_len {
                    if let Some(lookup) = length_hm.get(&i) {
                        lookup.iter().for_each(|k2| {
                            let v2 = map2.get(k2).unwrap();

                            // No need to run distance functions if exactly the same
                            if k1 == k2 {
                                iproduct!(v1, v2).for_each(|(v1, v2)| {
                                    idxs.push((*v1, *v2));
                                });
                                return;
                            }

                            let dist = self.compute(&k1, &k2);

                            // Check vs. threshold
                            if dist <= max_distance {
                                iproduct!(v1, v2).for_each(|(v1, v2)| {
                                    idxs.push((*v1, *v2));
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

        sorted_unzip(idxs)
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
