use crate::utils::robj_index_map;
use extendr_api::prelude::*;
use rayon::iter::*;
use rayon::ThreadPool;
use rustc_hash::FxHashMap;

pub mod damerau_levenshtein;
pub mod hamming;
pub mod lcs;
pub mod levenshtein;
pub mod osa;

// Define a trait for string distance calculations
pub trait EditDistance: Send + Sync {
    fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        max_distance: &f64,
        pool: &rayon::ThreadPool,
    ) -> (Vec<usize>, Vec<f64>);

    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        pool: &ThreadPool,
    ) -> anyhow::Result<Vec<(usize, usize, f64)>> {
        let map1 = robj_index_map(left, left_key)?;
        let map2 = robj_index_map(right, right_key)?;

        let mut length_map: FxHashMap<usize, Vec<&str>> = FxHashMap::default();
        for key in map2.keys() {
            let key_len = key.len();
            length_map.entry(key_len).or_default().push(key);
        }

        let idxs: Vec<(usize, usize, f64)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| {
                    self.compare_one_to_many(k1, v1, &length_map, &map2, &max_distance)
                })
                .flatten()
                .collect()
        });

        Ok(idxs)
    }

    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        length_map: &FxHashMap<usize, Vec<&str>>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
        max_distance: &f64,
    ) -> Option<Vec<(usize, usize, f64)>>;
}
