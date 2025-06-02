use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use std::cmp::max;
use std::collections::HashMap;
use textdistance::str::levenshtein;

/// Performs a fuzzy join between two data frames based on approximate string matching.
///
/// This function matches records in `df1` and `df2` using a specified column, allowing
/// matches within a given Levenshtein distance (`max_distance`). It builds index maps
/// for efficient lookup and comparison.
///
/// # Parameters
///
/// - `df1` (`List`): The first data frame. Note dataframes are a list of equal-length vectors.
/// - `df2` (`List`): The second data frame.
/// - `by` (`Vec<String>`): A vector containing column names to join on.
///   - `by[0]`: Column name in `df1`.
///   - `by[1]`: Column name in `df2`.
/// - `max_distance` (`i32`): The maximum allowable Levenshtein distance for a match.
///
/// # Returns
///
/// - `Robj`: A data frame containing matched records from both `df1` and `df2`,
///   with column names suffixed as `.x` (from `df1`) and `.y` (from `df2`).
///
/// # Example
///
/// ```rust
/// use extendr_api::prelude::*;
///
/// fn main() {
///     let df1 = List::from_values(vec![
///         ("name".to_string(), Robj::from(vec!["apple", "banana", "cherry"]))
///     ]);
///
///     let df2 = List::from_values(vec![
///         ("name".to_string(), Robj::from(vec!["appl", "bananna", "berry"]))
///     ]);
///
///     let result = fuzzy_join(df1, df2, vec!["name".to_string(), "name".to_string()], 2);
///     println!("{:?}", result);
/// }
/// ```
///
/// # Notes
///
/// - This function leverages **parallel iteration** (`par_iter()`) for performance optimization.
/// - Ensures minimal redundant comparisons by only comparing unique values.
/// - Only compares strings whose difference in length is within the user-specified max distance.
/// - The resulting data frame maintains column alignment while allowing fuzzy matching.
///
/// # See Also
///
/// - [`levenshtein`](https://docs.rs/levenshtein/latest/levenshtein/) - Used for calculating string distances.
/// - [`extendr`](https://extendr.github.io/) - Enables Rust to interface with R.
/// - [`data_frame!`](https://extendr.github.io/) - Constructs an R-compatible data frame.
///
/// @export
#[extendr]
fn fozzie_join(df1: List, df2: List, by: Vec<String>, max_distance: i32) -> Robj {
    let md = max_distance as usize;

    // It's not uncommon to have the same string listed many times
    // Keep a list of indices for each string so comps only happen once

    // Left-hand side
    let mut map1: HashMap<&str, Vec<usize>> = HashMap::new();
    df1.dollar(&by[0])
        .unwrap()
        .as_str_iter()
        .unwrap()
        .enumerate()
        .for_each(|(index, val)| {
            map1.entry(val)
                .and_modify(|v| v.push(index + 1))
                .or_insert(vec![index + 1]);
        });

    // Right-hand side
    let mut map2: HashMap<&str, Vec<usize>> = HashMap::new();
    df2.dollar(&by[1])
        .unwrap()
        .as_str_iter()
        .unwrap()
        .enumerate()
        .for_each(|(index, val)| {
            map2.entry(val)
                .and_modify(|v| v.push(&index + 1))
                .or_insert(vec![index + 1]);
        });

    // We don't need to check any strings where lengths differ by more than max
    // For RHS, keep a map of lengths of all strings
    // We use this later to subset the columns we compare in each set
    let mut length_hm: HashMap<usize, Vec<&str>> = HashMap::new();
    for key in map2.keys() {
        let key_len = key.len();
        length_hm.entry(key_len).or_insert(Vec::new()).push(key);
    }

    // Begin generation of all matched indices
    let (idx1, idx2): (Vec<usize>, Vec<usize>) = map1
        .par_iter()
        .filter_map(|(k1, v1)| {
            // Get range of lengths within max distance of current
            let k1_len = k1.len();
            let start_len = max(k1_len - md, 0);
            let end_len = k1_len + md + 1;

            // Start a list to collect results
            let mut idxs: Vec<(usize, usize)> = Vec::new();

            // Begin making string comparisons
            for i in start_len..end_len {
                if let Some(lookup) = length_hm.get(&i) {
                    lookup.iter().for_each(|k2| {
                        let v2 = map2.get(k2).unwrap();

                        // Run comparison, return idxs if match
                        if k1 == k2 || levenshtein(&k1, &k2) <= md {
                            iproduct!(v1, v2).for_each(|(v1, v2)| {
                                idxs.push((*v1, *v2));
                            });
                        }
                    });
                }
            }

            // Return all matches, if any found
            if idxs.is_empty() {
                return None;
            } else {
                return Some(idxs);
            }
        })
        .flatten()
        .unzip();

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
    fn fozzie_join;
}
