use extendr_api::prelude::*;
use std::collections::HashMap;

pub mod edit;
pub mod merge;
pub mod ngram;
pub mod normalized;
pub mod utils;

use edit::{DamerauLevenshtein, EditDistance, Hamming, Levenshtein, OSA};
use merge::Merge;
use ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use normalized::{Jaro, JaroWinkler, NormalizedEditDistance};
use utils::robj_index_map;

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
///   - `"jaro"` - Jaro similarity.
/// - `q` (`Option<i32>`): *q*-gram size (required for `"qgram"`, `"cosine"`, and `"jaccard"`).
/// - `max_distance` (`f64`): Maximum allowable edit distance.
/// - `how` (`String`): Specifies the join type (`"inner"`, `"left"`, `"right"`).
///   - `"inner"` (default): Returns only matching records.
///   - `"left"`: Returns all records from `df1`, with fuzzy matches from `df2`.
///   - `"right"`: Returns all records from `df2`, with fuzzy matches from `df1`.
/// - `distance_col` (`Option<String>`): Column name to store computed distance values.
///   If `None`, distances are not stored.
/// - `p` (`Option<i32>`): Used for fine-tuning certain similarity calculations.
/// - `bt` (`Option<f32>`): A threshold parameter influencing similarity computations.
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
    by: HashMap<String, String>,
    method: String,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    q: Option<i32>,
    p: Option<i32>,
    bt: Option<f32>,
) -> Robj {
    // Check for type of join requested
    match how.as_str() {
        "inner" => (),
        "left" => (),
        "right" => (),
        _ => panic!("{how} is not currently a supported join type."),
    }

    // It's not uncommon to have the same string listed many times
    // Keep a list of indices for each string so comps only happen once
    let keys: Vec<(&str, &str)> = by.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();

    // Convert the join key into a hashmap (string + vec occurrence indices)
    let map1 = robj_index_map(&df1, &keys[0].0);
    let map2 = robj_index_map(&df2, &keys[0].1);

    // For metrics requiring qgrams, check whether a Q was supplied
    let (idx1, idx2, dist) = match method.as_str() {
        "osa" => OSA.fuzzy_indices(map1, map2, max_distance),
        "levenshtein" | "lv" => Levenshtein.fuzzy_indices(map1, map2, max_distance),
        "damerau_levensthein" | "dl" => DamerauLevenshtein.fuzzy_indices(map1, map2, max_distance),
        "hamming" => Hamming.fuzzy_indices(map1, map2, max_distance),
        //"lcs" => LCSStr.fuzzy_indices(map1, map2, max_distance as usize),
        "qgram" => {
            if let Some(qz) = q {
                QGram.fuzzy_indices(map1, map2, max_distance, qz as usize)
            } else {
                panic!("Must provide q for method {}", method);
            }
        }
        "cosine" => {
            if let Some(qz) = q {
                Cosine.fuzzy_indices(map1, map2, max_distance, qz as usize)
            } else {
                panic!("Must provide q for method {}", method);
            }
        }
        "jaccard" => {
            if let Some(qz) = q {
                Jaccard.fuzzy_indices(map1, map2, max_distance, qz as usize)
            } else {
                panic!("Must provide q for method {}", method);
            }
        }
        "jaro_winkler" | "jw" => JaroWinkler.fuzzy_indices(map1, map2, max_distance),
        "jaro" => Jaro.fuzzy_indices(map1, map2, max_distance),
        _ => panic!("The join method {how} is not available."),
    };

    let out = match how.as_str() {
        "inner" => Merge::inner(&df1, &df2, idx1, idx2, distance_col, &dist),
        "left" => Merge::left(&df1, &df2, idx1, idx2, distance_col, &dist),
        "right" => Merge::right(&df1, &df2, idx1, idx2, distance_col, &dist),
        _ => panic!("Join type not supported"),
    };
    return out;
}

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_join_rs;
}
