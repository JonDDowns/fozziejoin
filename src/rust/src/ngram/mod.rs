use crate::utils::sort_unzip_triplet;
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use std::collections::HashMap;
pub mod cosine;
pub mod jaccard;
pub mod qgram;

// Define a trait for string distance calculations
pub trait QGramDistance: Send + Sync {
    fn compute(&self, s1: &str, s2: &str, q: usize) -> f64;

    fn fuzzy_indices(
        &self,
        map1: HashMap<&str, Vec<usize>>,
        map2: HashMap<&str, Vec<usize>>,
        max_distance: f64,
        q: usize,
    ) -> (Vec<usize>, Vec<usize>, Vec<Option<f64>>) {
        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                if k1.is_na() {
                    return None;
                }

                let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

                for (k2, v2) in map2.iter() {
                    if k2.is_na() {
                        continue;
                    }
                    if k2.len() < q {
                        continue;
                    }

                    if k1 == k2 {
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, Some(0.)));
                        });
                        continue;
                    }

                    let dist = self.compute(&k1, &k2, q);

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

// Helper function to generate q-grams
fn get_qgrams(s: &str, q: usize) -> HashMap<String, usize> {
    let chars: Vec<char> = s.chars().collect(); // Convert to character vector
    let mut qgram_map = HashMap::new();

    if q > chars.len() {
        return qgram_map; // Return empty map if q is larger than string length
    }

    for window in chars.windows(q) {
        let qgram: String = window.iter().collect();
        *qgram_map.entry(qgram).or_insert(0) += 1;
    }

    qgram_map
}
