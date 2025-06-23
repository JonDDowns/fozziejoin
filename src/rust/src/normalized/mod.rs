pub mod jaro_winkler;

use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;

pub trait NormalizedEditDistance: Send + Sync {
    fn compute(&self, s1: &str, s2: &str) -> f64;

    #[cfg(not(target_os = "windows"))]
    fn fuzzy_indices(
        &self,
        map1: HashMap<&str, Vec<usize>>,
        map2: HashMap<&str, Vec<usize>>,
        max_distance: f64,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
        if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
        };

        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                self.word_map_lookup_and_compare(k1, v1, &map2, full, max_distance)
            })
            .flatten()
            .collect();
        idxs
    }

    #[cfg(target_os = "windows")]
    fn fuzzy_indices(
        &self,
        map1: HashMap<&str, Vec<usize>>,
        map2: HashMap<&str, Vec<usize>>,
        max_distance: f64,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
        let nt = if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
            nt
        } else {
            rayon::current_num_threads()
        };

        let batch_size = map1.len().div_ceil(nt);

        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .iter()
            .collect::<Vec<(&&str, &Vec<usize>)>>()
            .par_chunks(batch_size)
            .flat_map_iter(|chunk| {
                chunk.iter().filter_map(|(k1, v1)| {
                    self.word_map_lookup_and_compare(k1, v1, &map2, full, max_distance)
                })
            })
            .flatten()
            .collect();
        idxs
    }

    fn word_map_lookup_and_compare(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        idx_map: &HashMap<&str, Vec<usize>>,
        full: bool,
        max_distance: f64,
    ) -> Option<Vec<(usize, usize, Option<f64>)>> {
        // If NA value, can skip all further checks
        if k1.is_na() && !full {
            return None;
        }
        let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

        for (k2, v2) in idx_map.iter() {
            // If comparison is NA string, skip
            if k2.is_na() && !full {
                continue;
            }
            if &k1 == k2 {
                iproduct!(v1, v2).for_each(|(v1, v2)| {
                    idxs.push((*v1, *v2, Some(0.)));
                });
                continue;
            }

            let dist = self.compute(&k1, &k2);

            if dist <= max_distance || full {
                iproduct!(v1, v2).for_each(|(a, b)| {
                    idxs.push((*a, *b, Some(dist)));
                });
                continue;
            }
        }

        if idxs.is_empty() {
            None
        } else {
            Some(idxs)
        }
    }
}
