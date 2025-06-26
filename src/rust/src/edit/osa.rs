// All text distance algorithms either directly use or import the
// the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::edit::EditDistance;
use crate::utils::robj_index_map;
use extendr_api::prelude::*;
use itertools::iproduct;
use std::collections::HashMap;

use rayon::iter::*;
use rayon::ThreadPoolBuilder;

pub struct OSA;
impl EditDistance for OSA {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        let s1: Vec<char> = s1.chars().collect();
        let s2: Vec<char> = s2.chars().collect();
        let l1 = s1.len();
        let l2 = s2.len();

        let mut mat: Vec<Vec<usize>> = vec![vec![0; l2 + 2]; l1 + 2];

        for i in 0..=l1 {
            mat[i][0] = i;
        }

        for i in 0..=l2 {
            mat[0][i] = i;
        }

        for (i1, c1) in s1.iter().enumerate() {
            for (i2, c2) in s2.iter().enumerate() {
                let sub_cost = if c1 == c2 { 0 } else { 1 };

                mat[i1 + 1][i2 + 1] = (mat[i1][i2 + 1] + 1)
                    .min(mat[i1 + 1][i2] + 1)
                    .min(mat[i1][i2] + sub_cost);

                if i1 == 0 || i2 == 0 {
                    continue;
                };

                if c1 != &s2[i2 - 1] {
                    continue;
                };

                if &s1[i1 - 1] != c2 {
                    continue;
                };

                let trans_cost = if c1 == c2 { 0 } else { 1 };

                mat[i1 + 1][i2 + 1] = mat[i1 + 1][i2 + 1].min(mat[i1 - 1][i2 - 1] + trans_cost);
            }
        }
        mat[l1][l2]
    }

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

        let mut map2: HashMap<&str, (Vec<usize>, Vec<char>)> = HashMap::new();

        let _ = right
            .dollar(right_key)
            .expect(&format!(
                "Column {right_key} does not exist or is not string."
            ))
            .as_str_iter()
            .expect(&format!(
                "Column {right_key} does not exist or is not string."
            ))
            .enumerate()
            .for_each(|(index, val)| {
                let outval: Vec<char> = val.chars().collect();
                map2.entry(val)
                    .and_modify(|v| v.0.push(index + 1))
                    .or_insert((vec![index + 1], outval));
            });

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
        let mut length_map: HashMap<usize, Vec<(&str, &Vec<char>)>> = HashMap::new();
        for (key, item) in map2.iter() {
            let key_len = key.len();
            length_map
                .entry(key_len)
                .or_insert(Vec::new())
                .push((key, &item.1));
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
                let val1: Vec<char> = k1.chars().collect();
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
                    false => k1_len.saturating_sub(max_distance as usize),
                };
                let end_len = match full {
                    true => *max_key + 1,
                    false => k1_len.saturating_add(max_distance as usize + 1),
                };

                // Start a list to collect results
                let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

                // Begin making string comparisons
                for i in start_len..end_len {
                    if let Some(lookup) = length_map.get(&i) {
                        lookup.iter().for_each(|(k2, chars)| {
                            // Skip this iter if RHS is NA
                            if k2.is_na() && full {
                                let v2 = map2.get(k2).unwrap();
                                iproduct!(v1, &v2.0).for_each(|(v1, v2)| {
                                    idxs.push((*v1, *v2, NA_REAL));
                                });
                                return;
                            }

                            // No need to run distance functions if exactly the same
                            if k1 == k2 {
                                let v2 = map2.get(k2).unwrap();
                                iproduct!(v1, &v2.0).for_each(|(v1, v2)| {
                                    idxs.push((*v1, *v2, Some(0.)));
                                });
                                return;
                            }

                            // Run distance calculation
                            let dist = self.compute_char(&val1, chars) as f64;

                            // Check vs. threshold
                            if dist <= max_distance || full {
                                let v2 = map2.get(k2).unwrap();
                                iproduct!(v1, &v2.0).for_each(|(v1, v2)| {
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
        idxs
    }

    fn compute_char(&self, s1: &Vec<char>, s2: &Vec<char>) -> usize {
        let l1 = s1.len();
        let l2 = s2.len();

        let mut mat: Vec<Vec<usize>> = vec![vec![0; l2 + 2]; l1 + 2];

        for i in 0..=l1 {
            mat[i][0] = i;
        }

        for i in 0..=l2 {
            mat[0][i] = i;
        }

        for (i1, c1) in s1.iter().enumerate() {
            for (i2, c2) in s2.iter().enumerate() {
                let sub_cost = if c1 == c2 { 0 } else { 1 };

                mat[i1 + 1][i2 + 1] = (mat[i1][i2 + 1] + 1)
                    .min(mat[i1 + 1][i2] + 1)
                    .min(mat[i1][i2] + sub_cost);

                if i1 == 0 || i2 == 0 {
                    continue;
                };

                if c1 != &s2[i2 - 1] {
                    continue;
                };

                if &s1[i1 - 1] != c2 {
                    continue;
                };

                let trans_cost = if c1 == c2 { 0 } else { 1 };
                mat[i1 + 1][i2 + 1] = mat[i1 + 1][i2 + 1].min(mat[i1 - 1][i2 - 1] + trans_cost);
            }
        }
        mat[l1][l2]
    }
}
