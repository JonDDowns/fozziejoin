pub mod jaro_winkler;

use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;

pub trait NormalizedEditDistance: Send + Sync {
    /// Compute approximate matches between two pre-indexed string maps using a fuzzy distance algorithm.
    ///
    /// This method compares the keys in `map1` to those in `map2` using a configurable fuzzy matching
    /// strategy. Each key maps to one or more row indices, and matches are returned as all index pairs
    /// whose keys meet a similarity threshold. Comparisons are parallelized for performance.
    ///
    /// # Parameters
    ///
    /// - `map1`: A `HashMap` of unique string tokens to row indices (e.g., from the left data frame).
    /// - `map2`: A second `HashMap` of string tokens to row indices (e.g., from the right data frame).
    /// - `max_distance`: The maximum acceptable distance score for two strings to be considered a match.
    /// - `full`: If `true`, includes all combinations regardless of overlap, used for `"full"` joins.
    /// - `nthread`: Optional number of threads to use for parallel execution. Defaults to Rayon’s global pool if `None`.
    ///
    /// # Returns
    ///
    /// A `Vec` of match tuples:
    /// - `(left_idx, right_idx, distance)` where
    ///   - `left_idx` is a row index from `map1`
    ///   - `right_idx` is a row index from `map2`
    ///   - `distance` is the computed similarity metric (or `None` for exact matches or skipped metrics)
    ///
    /// # Notes
    ///
    /// - Matching is implemented in `word_map_lookup_and_compare`, which is responsible for the
    ///   actual distance logic and result formatting.
    /// - Comparisons are run in parallel using Rayon’s `par_iter`.
    /// - Index maps should be precomputed to avoid repeated token parsing.
    ///
    fn fuzzy_indices(
        &self,
        map1: HashMap<&str, Vec<usize>>,
        map2: HashMap<&str, Vec<usize>>,
        max_distance: f64,
        full: bool,
        nthread: Option<usize>,
        prefix_weight: f64,
        max_prefix: usize,
    ) -> Vec<(usize, usize, Option<f64>)> {
        // If user specified a number of threads, build a custom pool
        if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
        };

        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                self.compare_one_to_many(
                    k1,
                    v1,
                    &map2,
                    full,
                    max_distance,
                    prefix_weight,
                    max_prefix,
                )
            })
            .flatten()
            .collect();
        idxs
    }

    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        idx_map: &HashMap<&str, Vec<usize>>,
        full: bool,
        max_distance: f64,
        prefix_weight: f64,
        max_prefix: usize,
    ) -> Option<Vec<(usize, usize, Option<f64>)>>;
}
