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

/// Transpose a sparse 2D matrix-like structure stored in a HashMap.
///
/// This function takes a `HashMap` where each key is a tuple `(usize, usize)`
/// representing a coordinate (e.g., row and column), and each value is a `Vec`
/// of optional `f64` values associated with that coordinate. It sorts the entries
/// by key, separates the coordinate keys into two separate vectors, and transposes
/// the vector of values so that each index of the inner vectors is aligned across
/// all entries.
///
/// # Arguments
///
/// * `data` - A HashMap mapping a coordinate tuple to a vector of optional f64 values.
///            Typically represents sparse entries in a 3D matrix or a list of distances.
///
/// # Returns
///
/// A tuple containing:
/// * `Vec<usize>`: All first elements from the coordinate keys (e.g., row indices).
/// * `Vec<usize>`: All second elements from the coordinate keys (e.g., column indices).
/// * `Vec<Vec<Option<f64>>>`: Transposed vectors of the original values. Each inner vector
///                             represents one "layer" across all coordinate entries.
///
/// # Example
///
/// ```
/// use std::collections::HashMap;
/// use fozziejoin::utils::transpose_map;
///
/// let mut data = HashMap::new();
/// data.insert((0, 1), vec![Some(1.0), None]);
/// data.insert((1, 0), vec![Some(2.0), Some(3.0)]);
///
/// let (keys1, keys2, transposed) = transpose_map(data);
///
/// assert_eq!(keys1, vec![0, 1]);
/// assert_eq!(keys2, vec![1, 0]);
/// assert_eq!(transposed[0], vec![Some(1.0), Some(2.0)]);
/// assert_eq!(transposed[1], vec![None, Some(3.0)]);
/// ```
pub fn transpose_map(
    data: HashMap<(usize, usize), Vec<Option<f64>>>,
) -> (Vec<usize>, Vec<usize>, Vec<Vec<Option<f64>>>) {
    // Convert the HashMap into a sorted Vec by key
    let mut sorted_entries: Vec<((usize, usize), Vec<Option<f64>>)> = data.into_iter().collect();
    sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

    // Initialize our 3 output vectors
    let mut keys1 = Vec::new();
    let mut keys2 = Vec::new();
    let mut transposed_values: Vec<Vec<Option<f64>>> = Vec::new();

    // How many distances do we have for each pair?
    let max_len = sorted_entries
        .iter()
        .map(|(_, v)| v.len())
        .max()
        .unwrap_or(0);
    transposed_values.resize(max_len, Vec::new());

    // Populate output vectors
    for ((key1, key2), values) in sorted_entries {
        keys1.push(key1);
        keys2.push(key2);

        for (i, &val) in values.iter().enumerate() {
            transposed_values[i].push(val);
        }
    }

    // Return outputs
    (keys1, keys2, transposed_values)
}

pub fn strvec_to_qgram_map<'a>(
    df: &'a List,
    key: &'a str,
    q: usize,
) -> HashMap<&'a str, (HashMap<&'a str, usize>, Vec<usize>)> {
    let mut qgram_map: HashMap<&'a str, (HashMap<&'a str, usize>, Vec<usize>)> = HashMap::new();

    df.dollar(key)
        .expect(&format!("Column {key} does not exist or is not string."))
        .as_str_iter()
        .expect(&format!("Column {key} does not exist or is not string."))
        .enumerate()
        .for_each(|(index, val)| {
            let hm: HashMap<&str, usize> = get_qgrams(val, q);
            qgram_map
                .entry(val)
                .and_modify(|v| v.1.push(index + 1))
                .or_insert((hm, vec![index + 1]));
        });

    qgram_map
}

pub fn get_qgrams(s: &str, q: usize) -> HashMap<&str, usize> {
    let mut qgram_map = HashMap::new();

    if s.len() < q {
        return qgram_map;
    }

    let mut char_indices = s.char_indices().collect::<Vec<_>>();
    char_indices.push((s.len(), '\0')); // Sentinel to get the final slice

    for i in 0..=char_indices.len().saturating_sub(q + 1) {
        let start = char_indices[i].0;
        let end = char_indices[i + q].0;
        let qgram = &s[start..end];
        *qgram_map.entry(qgram).or_insert(0) += 1;
    }

    qgram_map
}
