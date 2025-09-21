use crate::utils::robj_index_map;
use extendr_api::prelude::*;
use rayon::iter::*;
use rayon::ThreadPool;
use std::collections::HashMap;

pub mod damerau_levenshtein;
pub mod hamming;
pub mod lcs;
pub mod levenshtein;
pub mod osa;

// Define a trait for string distance calculations
pub trait EditDistance: Send + Sync {
    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        pool: &ThreadPool,
    ) -> Vec<(usize, usize, Option<f64>)> {
        let map1 = robj_index_map(left, left_key);
        let map2 = robj_index_map(right, right_key);

        // We don't need to check any strings where lengths differ by more than max
        // For RHS, keep a map of lengths of all strings
        // We use this later to subset the columns we compare in each set
        let mut length_map: HashMap<usize, Vec<&str>> = HashMap::new();
        for key in map2.keys() {
            let key_len = key.len();
            length_map.entry(key_len).or_insert(Vec::new()).push(key);
        }

        // Begin generation of all matched indices
        //let idxs: Vec<(usize, usize, Option<f64>)> =

        let idxs: Vec<(usize, usize, Option<f64>)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| {
                    self.compare_one_to_many(k1, v1, &length_map, &map2, &max_distance)
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
        length_map: &HashMap<usize, Vec<&str>>,
        idx_map: &HashMap<&str, Vec<usize>>,
        max_distance: &f64,
    ) -> Option<Vec<(usize, usize, Option<f64>)>>;
}
