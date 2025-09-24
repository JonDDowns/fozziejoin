use crate::utils::{get_qgrams, robj_index_map, strvec_to_qgram_map};
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use rayon::ThreadPool;
use std::collections::HashMap;
pub mod cosine;
pub mod jaccard;
pub mod qgram;

// Define a trait for string distance calculations
pub trait QGramDistance: Send + Sync {
    fn compute(&self, s1: &HashMap<&str, usize>, s2: &HashMap<&str, usize>) -> f64;

    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        q: usize,
        pool: &ThreadPool,
    ) -> Vec<(usize, usize, f64)> {
        let map1 = robj_index_map(&left, &left_key);

        // This map uses qgrams as keys and keeps track of both frequencies
        // and the number of occurrences of each qgram
        let map2_qgrams = strvec_to_qgram_map(right, right_key, q);

        let idxs: Vec<(usize, usize, f64)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| {
                    let out = self.compare_one_to_many(k1, v1, &map2_qgrams, q, max_distance);
                    out
                })
                .flatten()
                .collect()
        });
        idxs
    }

    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        map2_qgrams: &HashMap<&str, (HashMap<&str, usize>, Vec<usize>)>,
        q: usize,
        max_distance: f64,
    ) -> Option<Vec<(usize, usize, f64)>> {
        if k1.is_na() {
            return None;
        }

        let mut idxs: Vec<(usize, usize, f64)> = Vec::new();
        let qg1 = get_qgrams(k1, q);

        for (k2, (qg2, v2)) in map2_qgrams.iter() {
            if &k1 == k2 {
                iproduct!(v1, v2).for_each(|(v1, v2)| {
                    idxs.push((*v1, *v2, 0.));
                });
                continue;
            }

            let dist = self.compute(&qg1, &qg2) as f64;
            if dist <= max_distance {
                iproduct!(v1, v2).for_each(|(a, b)| {
                    idxs.push((*a, *b, dist));
                });
            }
        }

        if idxs.is_empty() {
            None
        } else {
            Some(idxs)
        }
    }
}
