use crate::utils::robj_index_map;
use anyhow::Result;
use extendr_api::prelude::*;
use itertools::iproduct;
use rapidfuzz::distance::jaro as jaro_rf;
use rayon::prelude::*;
use rayon::ThreadPool;
use rustc_hash::FxHashMap;

pub struct JaroWinkler;
impl JaroWinkler {
    pub fn fuzzy_indices(
        &self,
        df1: &List,
        left_key: &str,
        df2: &List,
        right_key: &str,
        max_distance: f64,
        prefix_weight: f64,
        max_prefix: usize,
        pool: &ThreadPool,
    ) -> Result<Vec<(usize, usize, f64)>> {
        let map1 = robj_index_map(&df1, left_key)?;
        let map2 = robj_index_map(&df2, right_key)?;

        let idxs: Vec<(usize, usize, f64)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| {
                    self.compare_one_to_many(k1, v1, &map2, max_distance, prefix_weight, max_prefix)
                })
                .flatten()
                .collect()
        });
        Ok(idxs)
    }

    pub fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        max_distance: &f64,
        prefix_weight: f64,
        max_prefix: usize,
        pool: &rayon::ThreadPool,
    ) -> (Vec<usize>, Vec<f64>) {
        let args = jaro_rf::Args::default().score_cutoff(*max_distance);
        let (keep, dists): (Vec<usize>, Vec<f64>) = pool.install(|| {
            left.par_iter()
                .zip(right)
                .enumerate()
                .filter_map(|(i, (l, r))| {
                    if l.is_na() || r.is_na() {
                        return None;
                    }
                    let dist: Option<f64> =
                        jaro_rf::distance_with_args(l.chars(), r.chars(), &args);
                    // Compute capped common prefix length
                    let capped_prefix_len = l
                        .chars()
                        .zip(r.chars())
                        .take_while(|(c1, c2)| c1 == c2)
                        .count()
                        .min(max_prefix);

                    match dist {
                        Some(x) => {
                            let x2 =
                                x + (capped_prefix_len as f64 * prefix_weight * (1.0 - x)) as f64;
                            if x2 <= *max_distance {
                                Some((i, x2))
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                })
                .unzip()
        });
        (keep, dists)
    }

    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
        max_distance: f64,
        prefix_weight: f64,
        max_prefix: usize,
    ) -> Option<Vec<(usize, usize, f64)>> {
        if k1.is_na() {
            return None;
        }

        let mut idxs: Vec<(usize, usize, f64)> = Vec::new();

        for (k2, v2) in idx_map.iter() {
            if k2.is_na() {
                continue;
            }

            if &k1 == k2 {
                iproduct!(v1, v2).for_each(|(v1, v2)| {
                    idxs.push((*v1, *v2, 0.));
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
                            idxs.push((*a, *b, x2));
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
