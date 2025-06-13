use crate::normalized::NormalizedEditDistance;
use textdistance::{Algorithm, JaroWinkler as TDJaroWinkler};

use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;

pub struct JaroWinkler {
    alg: TDJaroWinkler,
}

impl JaroWinkler {
    pub fn default() -> Self {
        Self {
            alg: TDJaroWinkler::default(),
        }
    }

    pub fn new(prefix_weight: f64, max_prefix: usize) -> Self {
        let mut alg = TDJaroWinkler::default();
        alg.max_prefix = max_prefix;
        alg.prefix_weight = prefix_weight;
        Self { alg }
    }
}

impl NormalizedEditDistance for JaroWinkler {
    fn compute(&self, s1: &str, s2: &str) -> f64 {
        self.alg.for_str(s1, s2).ndist()
    }

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
                // If NA value, can skip all further checks
                if k1.is_na() && !full {
                    return None;
                }
                let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

                for (k2, v2) in map2.iter() {
                    // If comparison is NA string, skip
                    if k2.is_na() && !full {
                        continue;
                    }
                    if k1 == k2 {
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
            })
            .flatten()
            .collect();
        idxs
    }
}
