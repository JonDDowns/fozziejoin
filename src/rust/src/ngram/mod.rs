use crate::utils::{get_qgrams, robj_index_map, strvec_to_qgram_map};
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
pub mod cosine;
pub mod jaccard;
pub mod qgram;

// Define a trait for string distance calculations
pub trait QGramDistance: Send + Sync {
    fn compute(&self, s1: &HashMap<&str, usize>, s2: &HashMap<&str, usize>) -> f64;

    /// Perform q-gram–based approximate string matching between two R data frame columns.
    ///
    /// This function compares tokens from the `left` data frame to q-gram–indexed values
    /// in the `right` data frame using a token similarity metric (e.g., cosine, jaccard).
    /// It computes distances for string pairs whose q-gram overlap exceeds a threshold,
    /// returning matched row indices and similarity scores.
    ///
    /// # Parameters
    ///
    /// - `left`: The first R data frame (as `extendr_api::List`).
    /// - `left_key`: Name of the column in `left` used for matching.
    /// - `right`: The second R data frame (as `extendr_api::List`).
    /// - `right_key`: Name of the column in `right` used for matching.
    /// - `max_distance`: Maximum allowable dissimilarity (e.g., `1 - similarity score`).
    /// - `q`: Q-gram size used for tokenization (e.g., 2 for bigrams).
    /// - `full`: Whether to include matches with no overlap (`true` for `"full"` join mode).
    /// - `nthread`: Optional number of threads to use for parallel matching.
    ///
    /// # Returns
    ///
    /// A `Vec` of tuples containing:
    /// - `left_idx`: Row index from the `left` data frame
    /// - `right_idx`: Row index from the `right` data frame
    /// - `Option<f64>`: The computed similarity or distance score for the matched pair
    ///
    /// # Notes
    ///
    /// - Q-gram comparison is optimized by precomputing a q-gram frequency index for `right_key`.
    /// - Matching is parallelized using Rayon for scalability across large data sets.
    /// - Uses a user-defined scoring function via `self.compare_string_to_qgram_map` to compute distances.
    /// - This version assumes the similarity algorithm (e.g. cosine, jaccard) is defined on the type that implements this method.
    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        q: usize,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
        if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
        };

        let map1 = robj_index_map(&left, &left_key);

        // This map uses qgrams as keys and keeps track of both frequencies
        // and the number of occurrences of each qgram
        let map2_qgrams = strvec_to_qgram_map(right, right_key, q);

        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                let out =
                    self.compare_string_to_qgram_map(full, k1, v1, &map2_qgrams, q, max_distance);
                out
            })
            .flatten()
            .collect();
        idxs
    }

    fn compare_string_to_qgram_map(
        &self,
        full: bool,
        k1: &str,
        v1: &Vec<usize>,
        map2_qgrams: &HashMap<&str, (HashMap<&str, usize>, Vec<usize>)>,
        q: usize,
        max_distance: f64,
    ) -> Option<Vec<(usize, usize, Option<f64>)>> {
        if !full {
            if k1.is_na() {
                return None;
            }
        }

        let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();
        let qg1 = get_qgrams(k1, q);

        for (k2, (qg2, v2)) in map2_qgrams.iter() {
            if &k1 == k2 {
                iproduct!(v1, v2).for_each(|(v1, v2)| {
                    idxs.push((*v1, *v2, Some(0.)));
                });
                continue;
            }

            let dist = self.compute(&qg1, &qg2) as f64;
            if dist <= max_distance || full {
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
    }
}
