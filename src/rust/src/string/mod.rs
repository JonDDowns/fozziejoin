pub mod edit;
pub mod ngram;
pub mod normalized;

use crate::string::edit::{
    damerau_levenshtein::DamerauLevenshtein, hamming::Hamming, lcs::LCSStr,
    levenshtein::Levenshtein, osa::OSA, EditDistance,
};
use crate::string::ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use crate::string::normalized::{jaro_winkler::JaroWinkler, NormalizedEditDistance};
use crate::utils::{get_pool, robj_index_map, transpose_map_fx};
use crate::Merge;

use extendr_api::prelude::*;
use rustc_hash::FxHashMap;

pub fn string_multi_join(
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
    let mut keep_idxs: FxHashMap<(usize, usize), Vec<f64>> = FxHashMap::default();

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
                .map(|(a, b, c)| ((*a, *b), vec![*c]))
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
    let (idxs1, idxs2, dists) = transpose_map_fx(keep_idxs);

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

pub fn string_single_join(
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
    let pool = get_pool(nthread);

    let n_by = by.len();
    if n_by > 1 {
        panic!("Uhoh");
    }

    let (left_key, right_key) = match by.iter().next() {
        Some((x, y)) => (x, y),
        None => panic!("No `by` arguments given"),
    };

    let rk = &right_key
        .as_str_vector()
        .expect(&format!("Error converting {:?} to string.", right_key))[0];

    let mut matchdat = match method.as_str() {
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
                Jaccard.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
            } else {
                panic!("Must provide q for method {}", method);
            }
        }
        "jaro_winkler" | "jw" => {
            let map1 = robj_index_map(&df1, &left_key);
            let map2 = robj_index_map(&df2, rk);

            let max_prefix: usize = max_prefix.expect("Parameter max_prefix not provided") as usize;
            let prefix_weight: f64 = prefix_weight.expect("Parameter prefix_weight not provided");

            let jw = JaroWinkler {};
            jw.fuzzy_indices(map1, map2, max_distance, prefix_weight, max_prefix, &pool)
        }
        _ => panic!("The join method {method} is not available."),
    };

    matchdat.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));

    let mut idxs1 = Vec::with_capacity(matchdat.len());
    let mut idxs2 = Vec::with_capacity(matchdat.len());
    let mut dists = Vec::with_capacity(matchdat.len());

    for (i, j, d) in matchdat {
        idxs1.push(i);
        idxs2.push(j);
        dists.push(d);
    }

    let out = match how.as_str() {
        "inner" => Merge::inner_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "left" => Merge::left_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "right" => Merge::right_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "anti" => Merge::anti(&df1, idxs1),
        "full" => Merge::full_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        _ => panic!("Join type not supported"),
    };

    out
}
