use crate::edit::EditDistance;
use core::f64;
use extendr_api::prelude::*;
use std::collections::HashMap;

pub mod edit;
pub mod merge;
pub mod ngram;
pub mod normalized;
pub mod utils;

use crate::edit::{
    damerau_levenshtein::DamerauLevenshtein, hamming::Hamming, lcs::LCSStr,
    levenshtein::Levenshtein, osa::OSA,
};
use crate::ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use crate::normalized::{jaro_winkler::JaroWinkler, NormalizedEditDistance};
use crate::utils::{get_pool, robj_index_map};

use merge::Merge;
use utils::transpose_map;

/// Perform a fuzzy join between two R data frames using approximate string similarity.
///
/// This function performs record linkage between `df1` and `df2`, allowing approximate
/// matches based on the specified join keys and string similarity metrics. The algorithm
/// efficiently computes pairwise distances using parallel iteration and indexed maps.
///
/// # Parameters
///
/// - `df1` (`List`): The first data frame (as an R list).
/// - `df2` (`List`): The second data frame (as an R list).
/// - `by` (`List`): Named list of matching columns. Each name-value pair maps a column
///   in `df1` to a corresponding column in `df2`.
/// - `method` (`String`): The similarity metric to use:
///   - `"levenshtein"`, `"lv"` – Levenshtein edit distance
///   - `"osa"` – Optimal string alignment
///   - `"damerau_levensthein"`, `"dl"` – Damerau-Levenshtein distance
///   - `"hamming"` – Hamming distance (requires equal-length strings)
///   - `"lcs"` – Longest common subsequence
///   - `"qgram"`, `"cosine"`, `"jaccard"` – Token-based methods (requires `q`)
///   - `"jaro_winkler"`, `"jw"` – Jaro-Winkler similarity (requires `max_prefix` & `prefix_weight`)
/// - `how` (`String`): Type of join:
///   - `"inner"` – Only matching rows from both inputs (default)
///   - `"left"` – All rows from `df1`, matched to `df2`
///   - `"right"` – All rows from `df2`, matched to `df1`
///   - `"anti"` – Only unmatched rows from `df1`
///   - `"full"` – All matches across both inputs
/// - `max_distance` (`f64`): Maximum similarity threshold. Depending on the method,
///   this acts as either a maximum distance or minimum similarity.
/// - `distance_col` (`Option<String>`): Optional name of a column to include per-match
///   distance or similarity score.
/// - `q` (`Option<i32>`): Gram size for `qgram`, `cosine`, and `jaccard`.
/// - `max_prefix` (`Option<i32>`): Required by `jaro_winkler`; controls prefix match scope.
/// - `prefix_weight` (`Option<f64>`): Required by `jaro_winkler`; adjusts prefix weight.
/// - `nthread` (`Option<usize>`): Number of threads to use (optional, defaults to all cores).
///
/// # Returns
///
/// An R-compatible data frame (`Robj`) containing the join result. Column names are suffixed:
/// - `.x` for values from `df1`
/// - `.y` for values from `df2`
///
/// If `distance_col` is specified, a numeric column is appended containing the calculated
/// similarity or distance for each match.
///
/// # Implementation Notes
///
/// - Internally uses indexed lookup tables to avoid full pairwise string comparisons.
/// - Supports multi-column matching and filters progressively across keys.
/// - Distance results are transposed and aligned across join keys using `transpose_map`.
///
/// # Dependencies
///
/// Relies on the `extendr` framework for R interop and uses an internal fuzzy matching trait
/// that differs by distance metric: qgrams, edit distance, or normalized edit distance.
///
/// @export
#[extendr]
pub fn fozzie_join_rs(
    df1: List,
    df2: List,
    by: List,
    method: String,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    q: Option<i32>,
    max_prefix: Option<i32>,
    prefix_weight: Option<f64>,
    nthread: Option<usize>,
) -> Robj {
    // Check for type of join requested
    let full = match how.as_str() {
        "inner" => false,
        "left" => false,
        "right" => false,
        "anti" => false,
        "full" => true,
        _ => panic!("{how} is not currently a supported join type."),
    };

    // Running list of all IDXs that have survived
    let mut keep_idxs: HashMap<(usize, usize), Vec<Option<f64>>> = HashMap::new();

    let pool = get_pool(nthread);

    // Begin looping through each supplied set of match keys
    for (match_iter, (left_key, right_key)) in by.iter().enumerate() {
        let rk = &right_key
            .as_str_vector()
            .expect(&format!("Error converting {:?} to string.", right_key))[0];

        // Get indices of all matching pairs using user-defined params.
        let matchdat = match method.as_str() {
            "osa" => OSA.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, full, &pool),
            "levenshtein" | "lv" => {
                Levenshtein.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, full, &pool)
            }
            "damerau_levensthein" | "dl" => DamerauLevenshtein.fuzzy_indices(
                &df1,
                left_key,
                &df2,
                rk,
                max_distance,
                full,
                &pool,
            ),
            "hamming" => Hamming.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, full, &pool),
            "lcs" => LCSStr.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, full, &pool),
            "qgram" => {
                if let Some(qz) = q {
                    QGram.fuzzy_indices(
                        &df1,
                        left_key,
                        &df2,
                        rk,
                        max_distance,
                        qz as usize,
                        full,
                        &pool,
                    )
                } else {
                    panic!("Must provide q for method {}", method);
                }
            }
            "cosine" => {
                if let Some(qz) = q {
                    Cosine.fuzzy_indices(
                        &df1,
                        left_key,
                        &df2,
                        rk,
                        max_distance,
                        qz as usize,
                        full,
                        &pool,
                    )
                } else {
                    panic!("Must provide q for method {}", method);
                }
            }
            "jaccard" => {
                if let Some(qz) = q {
                    Jaccard.fuzzy_indices(
                        &df1,
                        left_key,
                        &df2,
                        rk,
                        max_distance,
                        qz as usize,
                        full,
                        &pool,
                    )
                } else {
                    panic!("Must provide q for method {}", method);
                }
            }
            "jaro_winkler" | "jw" => {
                // Get hash map of unique words/indices of all occurrences
                let map1 = robj_index_map(&df1, &left_key);
                let map2 = robj_index_map(&df2, rk);

                // Extract max prefix size from args
                let max_prefix: usize = match max_prefix {
                    Some(x) => x as usize,
                    _ => panic!("Parameter max_prefix not provided"),
                };

                // Extract max prefix weight from args
                let prefix_weight: f64 = match prefix_weight {
                    Some(x) => x,
                    _ => panic!("Parameter prefix_weight not provided"),
                };

                // Define algorithm, run distance function
                let jw = JaroWinkler {};
                jw.fuzzy_indices(
                    map1,
                    map2,
                    max_distance,
                    full,
                    prefix_weight,
                    max_prefix,
                    &pool,
                )
            }
            _ => panic!("The join method {method} is not available."),
        };

        if match_iter == 0 {
            // On the first iteration, we initialize the idx's to keep
            keep_idxs = matchdat
                .iter()
                .map(|(a, b, c)| ((a.clone(), b.clone()), vec![c.clone()]))
                .collect();
        } else {
            // For everything else: only keep those surviving this and all prior rounds

            // Extract indices from current run
            let idxs: Vec<(usize, usize)> = matchdat.iter().map(|(a, b, _)| (*a, *b)).collect();

            // Anything that did not survive this iteration should be removed from contention
            keep_idxs.retain(|key, _| idxs.contains(key));

            // Add stringdistance result for survivors
            for (id1, id2, dist) in matchdat {
                if keep_idxs.contains_key(&(id1, id2)) {
                    keep_idxs.get_mut(&(id1, id2)).expect("hm").push(dist);
                }
            }
        }
    }

    // Reshape output to enable final DF creation
    let (idxs1, idxs2, dists) = transpose_map(keep_idxs);

    // Create the DF
    let out = match how.as_str() {
        "inner" | "full" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "anti" => Merge::anti(&df1, idxs1),
        _ => panic!("Join type not supported"),
    };

    // Final result
    return out;
}

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_join_rs;
}
