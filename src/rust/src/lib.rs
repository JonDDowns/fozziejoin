use core::f64;
use extendr_api::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;

pub mod difference;
pub mod interval;
pub mod merge;
pub mod string;
pub mod utils;

use crate::difference::fuzzy_indices_diff;
use crate::interval::fuzzy_indices_interval;
use crate::string::edit::{
    damerau_levenshtein::DamerauLevenshtein, hamming::Hamming, lcs::LCSStr,
    levenshtein::Levenshtein, osa::OSA, EditDistance,
};
use crate::string::ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use crate::string::normalized::{jaro_winkler::JaroWinkler, NormalizedEditDistance};
use crate::utils::{get_pool, robj_index_map};

use merge::Merge;
use utils::transpose_map;

/// Rust internal function performing string distance joins
/// @export
#[extendr]
pub fn fozzie_string_join_rs(
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
            "osa" => OSA.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
            "levenshtein" | "lv" => {
                Levenshtein.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool)
            }
            "damerau_levensthein" | "dl" => {
                DamerauLevenshtein.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool)
            }
            "hamming" => Hamming.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
            "lcs" => LCSStr.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
            "qgram" => {
                if let Some(qz) = q {
                    QGram.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
                } else {
                    panic!("Must provide q for method {}", method);
                }
            }
            "cosine" => {
                if let Some(qz) = q {
                    Cosine.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
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
                jw.fuzzy_indices(map1, map2, max_distance, prefix_weight, max_prefix, &pool)
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
        "inner" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "anti" => Merge::anti(&df1, idxs1),
        "full" => Merge::full(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        _ => panic!("Join type not supported"),
    };

    // Final result
    return out;
}

/// Rust internal function performing difference joins
/// @export
#[extendr]
pub fn fozzie_difference_join_rs(
    df1: List,
    df2: List,
    by: List,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    nthread: Option<usize>,
) -> Robj {
    // Running list of all IDXs that have survived
    let mut keep_idxs: HashMap<(usize, usize), Vec<Option<f64>>> = HashMap::new();

    let pool = get_pool(nthread);

    // Begin looping through each supplied set of match keys
    for (match_iter, (left_key, right_key)) in by.iter().enumerate() {
        let rk = &right_key
            .as_str_vector()
            .expect(&format!("Error converting {:?} to string.", right_key))[0];

        let vec1 = df1
            .dollar(left_key)
            .expect("Error extracting {left_key}")
            .as_real_vector()
            .expect("Error extracting {left_key}");
        let vec2 = df2
            .dollar(rk)
            .expect("Error extracting {rk}")
            .as_real_vector()
            .expect("Error extracting {rk}");
        let matchdat = fuzzy_indices_diff(vec1, vec2, max_distance, &pool);

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
        "inner" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "anti" => Merge::anti(&df1, idxs1),
        "full" => Merge::full(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        _ => panic!("Join type not supported"),
    };

    // Final result
    return out;
}

/// @export
#[extendr]
pub fn fozzie_interval_join_rs(
    df1: List,
    df2: List,
    by: List,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    nthread: Option<usize>,
) -> Robj {
    // Ensure exactly two named pairs are provided
    if by.len() != 2 {
        panic!("Interval join requires exactly two named pairs in `by` (start and end columns).");
    }

    let pool = get_pool(nthread);

    // Extract left and right keys
    let left_keys: Vec<String> = by
        .names()
        .expect("`by` must be a named list.")
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    let right_keys: Vec<String> = by
        .iter()
        .map(|(_, val)| {
            val.as_str_vector()
                .expect("Each `by` value must be a string vector.")
                .get(0)
                .expect("Empty string vector in `by`.")
                .to_string()
        })
        .collect();

    // Extract vectors from dataframe columns
    // Step 1: Extract and bind the column
    let right_end_col = df1
        .dollar(&left_keys[1])
        .expect("Error extracting right end column");

    // Step 2: Convert to iterator
    let vec1b = right_end_col
        .as_real_iter()
        .expect("Right end column must be numeric");

    let indexed_vec1: Vec<(usize, f64, f64)> = df1
        .dollar(&left_keys[0])
        .expect("Error extracting right start column")
        .as_real_iter()
        .expect("Right start column must be numeric")
        .zip(vec1b)
        .enumerate()
        .map(|(j, (start, end))| (j, start.min(*end), start.max(*end)))
        .collect();

    let right_end_col = df2
        .dollar(&right_keys[1])
        .expect("Error extracting right end column");

    // Step 2: Convert to iterator
    let vec2b = right_end_col
        .as_real_iter()
        .expect("Right end column must be numeric");

    let mut indexed_vec2: Vec<(usize, f64, f64)> = df2
        .dollar(&right_keys[0])
        .expect("Error extracting right start column")
        .as_real_iter()
        .expect("Right start column must be numeric")
        .zip(vec2b)
        .enumerate()
        .map(|(j, (start, end))| (j, start.min(*end), start.max(*end)))
        .collect();

    indexed_vec2.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    let matchdat = fuzzy_indices_interval(indexed_vec1, indexed_vec2, max_distance, &pool);

    // Collect results
    let keep_idxs: HashMap<(usize, usize), Vec<Option<f64>>> = matchdat
        .iter()
        .map(|(a, b, dist)| ((*a, *b), vec![dist.clone()]))
        .collect();

    let (idxs1, idxs2, dists) = transpose_map(keep_idxs);

    // Create the DF
    let out = match how.as_str() {
        "inner" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "anti" => Merge::anti(&df1, idxs1),
        "full" => Merge::full(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        _ => panic!("Join type not supported"),
    };

    out
}

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_string_join_rs;
    fn fozzie_difference_join_rs;
    fn fozzie_interval_join_rs;
}
