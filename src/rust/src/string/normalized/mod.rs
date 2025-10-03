pub mod jaro_winkler;

use rayon::prelude::*;
use rayon::ThreadPool;
use rustc_hash::FxHashMap;

pub trait NormalizedEditDistance: Send + Sync {
    fn fuzzy_indices(
        &self,
        map1: FxHashMap<&str, Vec<usize>>,
        map2: FxHashMap<&str, Vec<usize>>,
        max_distance: f64,
        prefix_weight: f64,
        max_prefix: usize,
        pool: &ThreadPool,
    ) -> Vec<(usize, usize, f64)> {
        let idxs: Vec<(usize, usize, f64)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| {
                    self.compare_one_to_many(k1, v1, &map2, max_distance, prefix_weight, max_prefix)
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
        idx_map: &FxHashMap<&str, Vec<usize>>,
        max_distance: f64,
        prefix_weight: f64,
        max_prefix: usize,
    ) -> Option<Vec<(usize, usize, f64)>>;

    fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        max_distance: &f64,
        prefix_weight: f64,
        max_prefix: usize,
    ) -> (Vec<usize>, Vec<f64>);
}
