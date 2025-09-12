use crate::utils::robj_index_map;
use extendr_api::prelude::*;
use rayon::iter::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;

pub mod damerau_levenshtein;
pub mod hamming;
pub mod lcs;
pub mod levenshtein;
pub mod osa;

// Define a trait for string distance calculations
pub trait EditDistance: Send + Sync {
    /// Perform approximate matching between two indexed string maps using a fuzzy distance metric.
    ///
    /// This method compares keys in `map1` against keys in `map2`, applying a fuzzy string
    /// similarity algorithm (implemented by the caller via `self.word_map_lookup_and_compare`)
    /// to generate matched index pairs whose distance is within `max_distance`. It filters
    /// comparisons based on string length proximity to improve efficiency.
    ///
    /// # Parameters
    ///
    /// - `map1`: A map from unique string tokens to their corresponding row indices (e.g., from `df1`).
    /// - `map2`: A second map of tokens to row indices (e.g., from `df2`), to match against.
    /// - `max_distance`: The maximum allowable string distance for a match to be considered valid.
    /// - `full`: If `true`, includes non-overlapping pairs in the join (used for `"full"` joins).
    /// - `nthread`: Optional number of threads to use for parallel distance calculation.
    ///
    /// # Returns
    ///
    /// A vector of matched index pairs in the form:
    /// - `(left_idx, right_idx, distance)` where
    ///   - `left_idx` refers to a row in `map1`
    ///   - `right_idx` refers to a row in `map2`
    ///   - `distance` is the computed similarity or distance (may be `None` for exact matches)
    ///
    /// # Implementation Notes
    ///
    /// - Uses string length filtering to prune unnecessary comparisons across maps.
    /// - Matches are computed in parallel via `rayon::par_iter`.
    /// - Requires the implementor to define `word_map_lookup_and_compare`, which encapsulates
    ///   the specific distance logic and output structure.
    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
        let map1 = robj_index_map(left, left_key);
        let map2 = robj_index_map(right, right_key);

        // If user specified a number of threads, build a custom pool
        if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
        };

        // We don't need to check any strings where lengths differ by more than max
        // For RHS, keep a map of lengths of all strings
        // We use this later to subset the columns we compare in each set
        let mut length_map: HashMap<usize, Vec<&str>> = HashMap::new();
        for key in map2.keys() {
            let key_len = key.len();
            length_map.entry(key_len).or_insert(Vec::new()).push(key);
        }

        // Get the min/max string lengths in the RHS dataset
        let min_key = length_map
            .keys()
            .min()
            .expect("Problem extracting minimum key length RHS");
        let max_key = length_map
            .keys()
            .max()
            .expect("Problem extracting maximum key length for RHS");

        // Begin generation of all matched indices
        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                self.compare_one_to_many(
                    k1,
                    v1,
                    &length_map,
                    &map2,
                    &full,
                    &max_distance,
                    min_key,
                    max_key,
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
        length_map: &HashMap<usize, Vec<&str>>,
        idx_map: &HashMap<&str, Vec<usize>>,
        full: &bool,
        max_distance: &f64,
        min_key: &usize,
        max_key: &usize,
    ) -> Option<Vec<(usize, usize, Option<f64>)>>;
}
