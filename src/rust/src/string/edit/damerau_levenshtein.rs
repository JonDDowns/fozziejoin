use crate::string::edit::EditDistance;
use extendr_api::prelude::*;
use itertools::iproduct;
use rapidfuzz::distance::damerau_levenshtein as dl_rf;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

pub struct DamerauLevenshtein;
impl EditDistance for DamerauLevenshtein {
    fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        max_distance: &f64,
        pool: &rayon::ThreadPool,
    ) -> (Vec<usize>, Vec<f64>) {
        let args = dl_rf::Args::default().score_cutoff(*max_distance as usize);
        let (keep, dists): (Vec<usize>, Vec<f64>) = pool.install(|| {
            left.par_iter()
                .zip(right)
                .enumerate()
                .filter_map(|(i, (l, r))| {
                    if l.is_na() || r.is_na() {
                        return None;
                    }
                    let dist = dl_rf::distance_with_args(l.chars(), r.chars(), &args);
                    let out = match dist {
                        None => None,
                        Some(x) => {
                            let x = x as f64;
                            if x <= *max_distance {
                                Some((i, x))
                            } else {
                                None
                            }
                        }
                    };
                    out
                })
                .unzip()
        });
        (keep, dists)
    }
    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        length_map: &FxHashMap<usize, Vec<&str>>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
        max_distance: &f64,
    ) -> Option<Vec<(usize, usize, f64)>> {
        // Skip all comparisons if string is NA
        if k1.is_na() {
            return None;
        }

        let scorer = dl_rf::BatchComparator::new(k1.chars());
        let args = dl_rf::Args::default().score_cutoff(*max_distance as usize);

        // Get range of lengths within max distance of current
        let k1_len = k1.len();
        let start_len = k1_len.saturating_sub(*max_distance as usize);
        let end_len = k1_len.saturating_add(*max_distance as usize + 1);

        // Start a list to collect results
        let mut idxs: Vec<(usize, usize, f64)> = Vec::new();

        // Begin making string comparisons
        for i in start_len..end_len {
            if let Some(lookup) = length_map.get(&i) {
                lookup.iter().for_each(|k2| {
                    // Skip this iter if RHS is NA
                    if k2.is_na() {
                        return;
                    }

                    // No need to run distance functions if exactly the same
                    if &k1 == k2 {
                        let v2 = idx_map.get(k2).unwrap();
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, 0.));
                        });
                        return;
                    }

                    // Run distance calculation
                    let dist = scorer.distance_with_args(k2.chars(), &args);

                    match dist {
                        Some(x) => {
                            let x = x as f64;
                            // Check vs. threshold
                            if x <= *max_distance {
                                let v2 = idx_map.get(k2).unwrap();
                                iproduct!(v1, v2).for_each(|(v1, v2)| {
                                    idxs.push((*v1, *v2, x as f64));
                                });
                                return;
                            }
                        }
                        None => (),
                    }
                });
            }
        }

        // Return all matches, if any
        if idxs.is_empty() {
            return None;
        } else {
            return Some(idxs);
        }
    }
}
