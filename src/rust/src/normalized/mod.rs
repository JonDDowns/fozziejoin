use crate::utils::sorted_unzip;
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
    ) -> (Vec<usize>, Vec<usize>) {
        let idxs: Vec<(usize, usize)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                let mut idxs: Vec<(usize, usize)> = Vec::new();

                for (k2, v2) in map2.iter() {
                    if k1 == k2 {
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2));
                        });
                        continue;
                    }

                    let dist = self.compute(&k1, &k2);

                    if dist <= max_distance {
                        iproduct!(v1, v2).for_each(|(a, b)| {
                            idxs.push((*a, *b));
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

        sorted_unzip(idxs)
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
