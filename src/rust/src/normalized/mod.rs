use crate::utils::sort_unzip_triplet;
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use std::collections::HashMap;
use textdistance::str::{jaro, jaro_winkler};

pub trait NormalizedEditDistance: Send + Sync {
    fn compute(&self, s1: &str, s2: &str) -> f64;
    fn fuzzy_indices(
        &self,
        map1: HashMap<&str, Vec<usize>>,
        map2: HashMap<&str, Vec<usize>>,
        max_distance: f64,
    ) -> (Vec<usize>, Vec<usize>, Vec<Option<f64>>) {
        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                // If NA value, can skip all further checks
                if k1.is_na() {
                    return None;
                }
                let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

                for (k2, v2) in map2.iter() {
                    // If comparison is NA string, skip
                    if k2.is_na() {
                        continue;
                    }
                    if k1 == k2 {
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, Some(0.)));
                        });
                        continue;
                    }

                    let dist = self.compute(&k1, &k2);

                    if dist <= max_distance {
                        iproduct!(v1, v2).for_each(|(a, b)| {
                            idxs.push((*a, *b, Some(dist)));
                        });
                    }
                }

                if idxs.is_empty() {
                    None
                } else {
                    Some(idxs)
                }
            })
            .flatten()
            .collect();

        sort_unzip_triplet(idxs)
    }
}

pub struct JaroWinkler;
impl NormalizedEditDistance for JaroWinkler {
    fn compute(&self, s1: &str, s2: &str) -> f64 {
        1.0 - jaro_winkler(s1, s2)
    }
}

pub struct Jaro;
impl NormalizedEditDistance for Jaro {
    fn compute(&self, s1: &str, s2: &str) -> f64 {
        1.0 - jaro(s1, s2)
    }
}
