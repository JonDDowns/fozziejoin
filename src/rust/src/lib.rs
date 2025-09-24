use core::f64;
use extendr_api::prelude::*;
use std::collections::HashMap;

pub mod difference;
pub mod merge;
pub mod string;
pub mod utils;

use crate::difference::fuzzy_indices_diff;
use crate::string::edit::{
    damerau_levenshtein::DamerauLevenshtein, hamming::Hamming, lcs::LCSStr,
    levenshtein::Levenshtein, osa::OSA, EditDistance,
};
use crate::string::ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use crate::string::normalized::{jaro_winkler::JaroWinkler, NormalizedEditDistance};
use crate::utils::{get_pool, robj_index_map};

use merge::Merge;
use utils::transpose_map;

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
            keep_idxs = matchdat;
        } else {
            for (key, match_vals) in &matchdat {
                if let Some(keep_vals) = keep_idxs.get_mut(key) {
                    keep_vals.extend(match_vals.iter().cloned());
                }
            }
            keep_idxs.retain(|key, _| matchdat.contains_key(key));
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

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_string_join_rs;
    fn fozzie_difference_join_rs;
}
