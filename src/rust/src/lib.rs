use extendr_api::prelude::*;
use std::collections::HashMap;

pub mod edit;
pub mod ngram;
pub mod normalized;
pub mod utils;

use edit::{DamerauLevenshtein, EditDistance, Hamming, LCSStr, Levenshtein, OSA};
use ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use normalized::{Jaro, JaroWinkler, NormalizedEditDistance};
use utils::robj_index_map;

/// Performs a fuzzy join between two data frames based on approximate string matching.
///
/// This function matches records in `df1` and `df2` using specified column names, allowing
/// matches within a given distance threshold using various fuzzy matching methods.
/// It constructs index maps for efficient lookups and comparisons, ensuring minimal
/// redundant calculations.
///
/// # Parameters
///
/// - `df1` (`List`): The first data frame (R List format), containing named vectors.
/// - `df2` (`List`): The second data frame.
/// - `by` (`HashMap<String, String>`): A mapping of column names specifying join keys:
///   - The key represents the column name in `df1`.
///   - The value represents the corresponding column name in `df2`.
/// - `method` (`String`): The fuzzy matching method to use. Supported methods:
///   - `"osa"` - Optimal string alignment distance.
///   - `"levenshtein"` or `"lv"` - Standard Levenshtein edit distance.
///   - `"damerau_levensthein"` or `"dl"` - Damerau-Levenshtein edit distance.
///   - `"hamming"` - Hamming distance (only works for equal-length strings).
///   - `"lcs"` - Longest common subsequence similarity.
///   - `"qgram"` - Q-gram comparison (requires `q` value).
///   - `"cosine"` - Cosine similarity (requires `q` value).
///   - `"jaccard"` - Jaccard similarity (requires `q` value).
///   - `"jaro_winkler"` or `"jw"` - Jaro-Winkler similarity.
///   - `"jaro"` - Standard Jaro similarity.
/// - `q` (`Option<i32>`): The *q*-gram size (required for `"qgram"`, `"cosine"`, and `"jaccard"` methods).
/// - `max_distance` (`f64`): Maximum allowable distance for a match.
///
/// # Returns
///
/// - `Robj`: A data frame containing matched records from `df1` and `df2`,
///   with column names suffixed as `.x` (from `df1`) and `.y` (from `df2`).
///
/// # Notes
///
/// - Uses **parallel iteration** (`par_iter()`) for efficient comparisons.
/// - Only evaluates string pairs where the difference in length is within `max_distance`.
/// - Minimizes redundant checks by using **indexed maps** instead of naive pairwise comparisons.
/// - Ensures column alignment while supporting fuzzy matching.
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
    q: Option<i32>,
    max_distance: f64,
) -> Robj {
    // It's not uncommon to have the same string listed many times
    // Keep a list of indices for each string so comps only happen once
    let keys: Vec<(&str, &str)> = by.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();

    // Left-hand side
    let map1 = robj_index_map(&df1, &keys[0].0);
    let map2 = robj_index_map(&df2, &keys[0].1);

    // Generate all matched indices
    // For qgrams, we need to extract the value (which could be null)
    let (idx1, idx2) = match method.as_str() {
        "osa" => OSA.fuzzy_indices(map1, map2, max_distance as usize),
        "levenshtein" | "lv" => Levenshtein.fuzzy_indices(map1, map2, max_distance as usize),
        "damerau_levensthein" | "dl" => {
            DamerauLevenshtein.fuzzy_indices(map1, map2, max_distance as usize)
        }
        "hamming" => Hamming.fuzzy_indices(map1, map2, max_distance as usize),
        "lcs" => LCSStr.fuzzy_indices(map1, map2, max_distance as usize),
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
        _ => panic!("Ruhroh"),
    };

    // Generate vectors of column names and R objects
    let num_cols: usize = df1.ncols() + df2.ncols();
    let mut names: Vec<String> = Vec::with_capacity(num_cols);
    let mut combined: Vec<Robj> = Vec::with_capacity(num_cols);

    // Subset to matched records in left-hand side, push to main list
    for (name, col1) in df1.iter() {
        let vals = col1.slice(&idx1).unwrap();
        names.push(name.to_string() + ".x");
        combined.push(vals);
    }

    // Subset to matched records in right-hand side, push to main list
    for (name, col2) in df2.iter() {
        let vals = col2.slice(&idx2).unwrap();
        names.push(name.to_string() + ".y");
        combined.push(vals);
    }

    // Final type conversions and output
    let out: Robj = List::from_names_and_values(names, combined)
        .unwrap()
        .as_robj()
        .clone();
    data_frame!(out)
}

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_join_rs;
}
}
