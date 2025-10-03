use crate::utils::robj_index_map;
use extendr_api::prelude::*;
use rayon::iter::*;
use rayon::ThreadPool;
use rustc_hash::FxHashMap;

pub trait StringDist: Send + Sync {
    type Config;

    fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        config: &Self::Config,
    ) -> (Vec<usize>, Vec<f64>);

    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        length_map: &FxHashMap<usize, Vec<&str>>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
        config: &Self::Config,
    ) -> Option<Vec<(usize, usize, f64)>>;

    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        config: &Self::Config,
        pool: &ThreadPool,
    ) -> Vec<(usize, usize, f64)>
    where
        Self::Config: Sync,
    {
        let map1 = robj_index_map(left, left_key);
        let map2 = robj_index_map(right, right_key);

        let mut length_map: FxHashMap<usize, Vec<&str>> = FxHashMap::default();
        for key in map2.keys() {
            length_map.entry(key.len()).or_default().push(key);
        }

        pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| self.compare_one_to_many(k1, v1, &length_map, &map2, config))
                .flatten()
                .collect()
        })
    }
}
