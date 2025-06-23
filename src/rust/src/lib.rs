use core::f64;
use extendr_api::prelude::*;
use std::collections::HashMap;

pub mod edit;
pub mod merge;
pub mod ngram;
pub mod normalized;
pub mod utils;

use edit::{DamerauLevenshtein, EditDistance, Hamming, LCSStr, Levenshtein, OSA};
use merge::Merge;
use ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use normalized::{jaro_winkler::JaroWinkler, NormalizedEditDistance};
use utils::{robj_index_map, transpose_map};

/// Performs a fuzzy join between two data frames using approximate string matching.
///
/// This function matches records in `df1` and `df2` using specified column names, allowing
/// matches within a given distance threshold. It supports various fuzzy matching methods and
/// constructs index maps for efficient lookups while minimizing redundant calculations.
///
/// # Parameters
///
/// - `df1` (`List`): The first data frame (R List format).
/// - `df2` (`List`): The second data frame.
/// - `by` (`HashMap<String, String>`): Specifies join keys:
///   - The key represents the column name in `df1`.
///   - The value represents the corresponding column name in `df2`.
/// - `method` (`String`): The fuzzy matching algorithm to use:
///   - `"levenshtein"` | `"lv"` - Levenshtein edit distance.
///   - `"osa"` - Optimal string alignment distance.
///   - `"damerau_levensthein"` | `"dl"` - Damerau-Levenshtein distance.
///   - `"hamming"` - Hamming distance (equal-length strings only).
///   - `"lcs"` - Longest common subsequence.
///   - `"qgram"` - Q-gram similarity (requires `q` parameter).
///   - `"cosine"` - Cosine similarity (requires `q` parameter).
///   - `"jaccard"` - Jaccard similarity (requires `q` parameter).
///   - `"jaro_winkler"` | `"jw"` - Jaro-Winkler similarity.
/// - `q` (`Option<i32>`): *q*-gram size (required for `"qgram"`, `"cosine"`, and `"jaccard"`).
/// - `max_distance` (`f64`): Maximum allowable edit distance.
/// - `how` (`String`): Specifies the join type (`"inner"`, `"left"`, `"right"`).
///   - `"inner"` (default): Returns only matching records.
///   - `"left"`: Returns all records from `df1`, with fuzzy matches from `df2`.
///   - `"right"`: Returns all records from `df2`, with fuzzy matches from `df1`.
/// - `distance_col` (`Option<String>`): Column name to store computed distance values.
///   If `None`, distances are not stored.
/// - `max_prefix` (`Option<i32>`): A threshold parameter influencing similarity computations.
/// - `prefix_weight` (`Option<f32>`): Used for fine-tuning certain similarity calculations.
///
/// # Returns
///
/// - `Robj`: A data frame containing matched records from `df1` and `df2`,
///   with column names suffixed as `.x` (from `df1`) and `.y` (from `df2`).
///   If `distance_col` is provided, the computed distance values will be included.
///
/// # Notes
///
/// - Uses **parallel iteration** (`par_iter()`) for efficient comparisons.
/// - Minimizes redundant checks by using **indexed maps** instead of naive pairwise comparisons.
/// - Supports multiple fuzzy matching techniques to enhance flexibility.
///
/// # Example
///
/// ```rust
/// let result = fozzie_join_rs(df1, df2, by, "levenshtein", "inner", 2.0, Some("dist"), None, None, None);
/// ```
///
/// # See Also
///
/// - [`levenshtein`](https://docs.rs/levenshtein/latest/levenshtein/) - Computes edit distances.
/// - [`extendr`](https://extendr.github.io/) - Enables Rust interoperability with R.
/// - [`data_frame!`](https://extendr.github.io/) - Constructs an R-compatible data frame.
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

    let mut keep_idxs: HashMap<(usize, usize), Vec<Option<f64>>> = HashMap::new();

    for (z, (left_key, right_key)) in by.iter().enumerate() {
        let rk = &right_key.as_str_vector().expect("lul")[0];

        // Convert the join key into a hashmap (string + vec occurrence indices)

        // For metrics requiring qgrams, check whether a Q was supplied
        let matchdat = match method.as_str() {
            "osa" => {
                let map1 = robj_index_map(&df1, &left_key);
                let map2 = robj_index_map(&df2, rk);
                OSA.fuzzy_indices(map1, map2, max_distance, full, nthread)
            }
            "levenshtein" | "lv" => {
                let map1 = robj_index_map(&df1, &left_key);
                let map2 = robj_index_map(&df2, rk);
                Levenshtein.fuzzy_indices(map1, map2, max_distance, full, nthread)
            }
            "damerau_levensthein" | "dl" => {
                let map1 = robj_index_map(&df1, &left_key);
                let map2 = robj_index_map(&df2, rk);
                DamerauLevenshtein.fuzzy_indices(map1, map2, max_distance, full, nthread)
            }
            "hamming" => {
                let map1 = robj_index_map(&df1, &left_key);
                let map2 = robj_index_map(&df2, rk);
                Hamming.fuzzy_indices(map1, map2, max_distance, full, nthread)
            }
            "lcs" => {
                let map1 = robj_index_map(&df1, &left_key);
                let map2 = robj_index_map(&df2, rk);
                LCSStr.fuzzy_indices(map1, map2, max_distance, full, nthread)
            }
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
                        nthread,
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
                        nthread,
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
                        nthread,
                    )
                } else {
                    panic!("Must provide q for method {}", method);
                }
            }
            "jaro_winkler" | "jw" => {
                let map1 = robj_index_map(&df1, &left_key);
                let map2 = robj_index_map(&df2, rk);
                let max_prefix: usize = match max_prefix {
                    Some(x) => x as usize,
                    _ => panic!("Parameter max_prefix not provided"),
                };
                let prefix_weight: f64 = match prefix_weight {
                    Some(x) => x,
                    _ => panic!("Parameter prefix_weight not provided"),
                };
                let jw = JaroWinkler::new(prefix_weight, max_prefix);
                jw.fuzzy_indices(map1, map2, max_distance, full, nthread)
            }
            _ => panic!("The join method {method} is not available."),
        };

        if z == 0 {
            keep_idxs = matchdat
                .iter()
                .map(|(a, b, c)| ((a.clone(), b.clone()), vec![c.clone()]))
                .collect();
        } else {
            let idxs: Vec<(usize, usize)> = matchdat.iter().map(|(a, b, _)| (*a, *b)).collect();
            keep_idxs.retain(|key, _| idxs.contains(key));
            for (id1, id2, dist) in matchdat {
                if keep_idxs.contains_key(&(id1, id2)) {
                    keep_idxs.get_mut(&(id1, id2)).expect("hm").push(dist);
                }
            }
        }
    }

    let (idxs1, idxs2, dists) = transpose_map(keep_idxs);

    let out = match how.as_str() {
        "inner" | "full" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "anti" => Merge::anti(&df1, idxs1),
        _ => panic!("Join type not supported"),
    };
    return out;
}

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_join_rs;
}
