use crate::EditDistance;
use extendr_api::prelude::*;
use itertools::iproduct;
use rapidfuzz::distance::levenshtein as lv_rf;
use std::collections::HashMap;

pub struct Levenshtein;
impl EditDistance for Levenshtein {
    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        length_map: &HashMap<usize, Vec<&str>>,
        idx_map: &HashMap<&str, Vec<usize>>,
        max_distance: &f64,
    ) -> Option<Vec<(usize, usize, Option<f64>)>> {
        // Skip all comparisons if string is NA
        if k1.is_na() {
            return None;
        }

        let scorer = lv_rf::BatchComparator::new(k1.chars());
        let args = lv_rf::Args::default().score_cutoff(*max_distance as usize);

        // Get range of lengths within max distance of current
        let k1_len = k1.len();
        let start_len = k1_len.saturating_sub(*max_distance as usize);
        let end_len = k1_len.saturating_add(*max_distance as usize + 1);

        // Start a list to collect results
        let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

        // Begin making string comparisons
        for i in start_len..end_len {
            if let Some(lookup) = length_map.get(&i) {
                lookup.iter().for_each(|k2| {
                    // Skip this iter if RHS is NA
                    if k2.is_na() {
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
                    let dist = scorer.distance_with_args(k2.chars(), &args);

                    match dist {
                        Some(x) => {
                            let x = x as f64;
                            // Check vs. threshold
                            if x <= *max_distance {
                                let v2 = idx_map.get(k2).unwrap();
                                iproduct!(v1, v2).for_each(|(v1, v2)| {
                                    idxs.push((*v1, *v2, Some(x as f64)));
                                });
                                return;
                            }
                        }
                        None => (),
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
