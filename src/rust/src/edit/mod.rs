use crate::utils::robj_index_map;
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::iter::*;
use rayon::prelude::*;
use std::collections::HashMap;
use textdistance::str::{
    damerau_levenshtein, damerau_levenshtein_restricted, hamming, levenshtein,
};

// Define a trait for string distance calculations
pub trait EditDistance: Send + Sync {
    fn compute(&self, s1: &str, s2: &str) -> usize;

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
        let num_groups: usize = match nthread {
            Some(x) => x,
            _ => rayon::current_num_threads(),
        };

        let md = max_distance as usize;

        // Convert the join key into a hashmap (string + vec occurrence indices)
        let map1 = robj_index_map(&left, &left_key);
        let map2 = robj_index_map(&right, &right_key);

        // We don't need to check any strings where lengths differ by more than max
        // For RHS, keep a map of lengths of all strings
        // We use this later to subset the columns we compare in each set
        let mut length_hm: HashMap<usize, Vec<&str>> = HashMap::new();
        for key in map2.keys() {
            let key_len = key.len();
            length_hm.entry(key_len).or_insert(Vec::new()).push(key);
        }
        let min_key = length_hm.keys().min().expect("Problem?!");
        let max_key = length_hm.keys().max().expect("Problem?!");

        let map1_vec: Vec<_> = map1.iter().collect();
        let chunk_size = map1_vec.len().div_ceil(num_groups);

        let idxs: Vec<(usize, usize, Option<f64>)> = map1_vec
            .par_chunks(chunk_size)
            .flat_map(|chunk| {
                let mut local_idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

                for (k1, v1) in chunk {
                    if !full && k1.is_na() {
                        continue;
                    }

                    let k1_len = k1.len();
                    let start_len = if full {
                        *min_key
                    } else {
                        k1_len.saturating_sub(md)
                    };
                    let end_len = if full {
                        *max_key + 1
                    } else {
                        k1_len.saturating_add(md + 1)
                    };

                    for i in start_len..end_len {
                        if let Some(lookup) = length_hm.get(&i) {
                            for k2 in lookup {
                                let v2 = map2.get(k2).unwrap();

                                if (k2.is_na() || k1.is_na()) && full {
                                    iproduct!(*v1, v2).for_each(|(v1, v2)| {
                                        local_idxs.push((*v1, *v2, NA_REAL));
                                    });
                                    continue;
                                }

                                if k1 == &k2 {
                                    iproduct!(*v1, v2).for_each(|(v1, v2)| {
                                        local_idxs.push((*v1, *v2, Some(0.)));
                                    });
                                    continue;
                                }

                                let dist = self.compute(k1, k2) as f64;
                                if dist <= max_distance || full {
                                    iproduct!(*v1, v2).for_each(|(v1, v2)| {
                                        local_idxs.push((*v1, *v2, Some(dist)));
                                    });
                                }
                            }
                        }
                    }
                }

                local_idxs
            })
            .collect();
        idxs
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
        damerau_levenshtein_restricted(s1, s2)
    }
}
