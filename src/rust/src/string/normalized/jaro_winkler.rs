// All text distance algorithms either directly use or import the
// the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::string::normalized::NormalizedEditDistance;
use extendr_api::prelude::*;
use itertools::iproduct;
use rapidfuzz::distance::jaro as jaro_rf;
use std::collections::HashMap;

pub struct JaroWinkler;
impl NormalizedEditDistance for JaroWinkler {
    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        idx_map: &HashMap<&str, Vec<usize>>,
        max_distance: f64,
        prefix_weight: f64,
        max_prefix: usize,
    ) -> Option<Vec<(usize, usize, Option<f64>)>> {
        if k1.is_na() {
            return None;
        }

        let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

        for (k2, v2) in idx_map.iter() {
            if k2.is_na() {
                continue;
            }

            if &k1 == k2 {
                iproduct!(v1, v2).for_each(|(v1, v2)| {
                    idxs.push((*v1, *v2, Some(0.)));
                });
                continue;
            }

            // Compute capped common prefix length
            let capped_prefix_len = k1
                .chars()
                .zip(k2.chars())
                .take_while(|(c1, c2)| c1 == c2)
                .count()
                .min(max_prefix);

            let scorer = jaro_rf::BatchComparator::new(k1.chars());
            let args = jaro_rf::Args::default().score_cutoff(max_distance);

            let dist = scorer.distance_with_args(k2.chars(), &args);
            match dist {
                Some(x) => {
                    let x2 = x + (capped_prefix_len as f64 * prefix_weight * (1.0 - x)) as f64;
                    if x2 <= max_distance {
                        iproduct!(v1, v2).for_each(|(a, b)| {
                            idxs.push((*a, *b, Some(x2)));
                        });
                    }
                }
                None => (),
            }
        }

        if idxs.is_empty() {
            None
        } else {
            Some(idxs)
        }
    }
}
