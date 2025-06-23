use extendr_api::prelude::*;
use std::collections::HashMap;

/// Constructs a HashMap that indexes occurrences of unique string values
/// in a specified column of an R data frame (`List`).
///
/// # Parameters:
/// - `df`: A reference to an `extendr_api::List` representing an R data frame.
/// - `key`: A string slice (`&str`) specifying the column to index.
///
/// # Returns:
/// A `HashMap` where:
/// - The keys are unique strings found in the specified column.
/// - The values are vectors of `usize` indices where the corresponding string appears.
///
/// # Panics:
/// - If the specified column does not exist or is not a string vector.
pub fn robj_index_map<'a>(df: &'a List, key: &'a str) -> HashMap<&'a str, Vec<usize>> {
    let mut map: HashMap<&str, Vec<usize>> = HashMap::new();

    let _ = df
        .dollar(key)
        .expect(&format!("Column {key} does not exist or is not string."))
        .as_str_iter()
        .expect(&format!("Column {key} does not exist or is not string."))
        .enumerate()
        .for_each(|(index, val)| {
            map.entry(val)
                .and_modify(|v| v.push(index + 1))
                .or_insert(vec![index + 1]);
        });

    map
}

pub fn transpose_map(
    data: HashMap<(usize, usize), Vec<Option<f64>>>,
) -> (Vec<usize>, Vec<usize>, Vec<Vec<Option<f64>>>) {
    // Convert the HashMap into a sorted Vec by key
    let mut sorted_entries: Vec<((usize, usize), Vec<Option<f64>>)> = data.into_iter().collect();
    sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut keys1 = Vec::new();
    let mut keys2 = Vec::new();
    let mut transposed_values: Vec<Vec<Option<f64>>> = Vec::new();

    // Determine the maximum vector length for handling uneven data
    let max_len = sorted_entries
        .iter()
        .map(|(_, v)| v.len())
        .max()
        .unwrap_or(0);
    transposed_values.resize(max_len, Vec::new());

    for ((key1, key2), values) in sorted_entries {
        keys1.push(key1);
        keys2.push(key2);

        for (i, &val) in values.iter().enumerate() {
            transposed_values[i].push(val);
        }
    }

    (keys1, keys2, transposed_values)
}
