use extendr_api::prelude::*;
use rayon::prelude::*;
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
        .dollar(key) // Extract column data from the R List
        .expect(&format!("Column {key} does not exist or is not string."))
        .as_str_iter() // Convert column values to an iterator of strings
        .expect(&format!("Column {key} does not exist or is not string."))
        .enumerate()
        .for_each(|(index, val)| {
            map.entry(val) // Insert or update mapping
                .and_modify(|v| v.push(index + 1)) // Add index if key exists
                .or_insert(vec![index + 1]); // Create new entry if key does not exist
        });

    map
}

/// Sorts a vector of `(usize, usize)` pairs by the first element, then the second.
/// Returns two separate sorted vectors.
///
/// # Parameters:
/// - `pairs`: A mutable vector containing pairs `(usize, usize)`.
///
/// # Returns:
/// A tuple `(Vec<usize>, Vec<usize>)` where:
/// - The first vector contains the first element of each sorted pair.
/// - The second vector contains the second element of each sorted pair.
///
/// # Example Usage:
/// ```rust
/// let pairs = vec![(2, 4), (1, 3), (1, 4)];
/// let (sorted_idx1, sorted_idx2) = sorted_unzip(pairs);
/// assert_eq!(sorted_idx1, vec![1, 1, 2]);
/// assert_eq!(sorted_idx2, vec![3, 4, 4]);
/// ```
pub fn sort_unzip_triplet(
    mut items: Vec<(usize, usize, Option<f64>)>,
) -> (Vec<usize>, Vec<usize>, Vec<Option<f64>>) {
    items.par_sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    // Initialize three separate vectors
    let mut idx1_sorted = Vec::with_capacity(items.len());
    let mut idx2_sorted = Vec::with_capacity(items.len());
    let mut dist_sorted = Vec::with_capacity(items.len());

    // Manually iterate over pairs and push values into vectors
    for (i1, i2, dist) in items {
        idx1_sorted.push(i1);
        idx2_sorted.push(i2);
        dist_sorted.push(dist);
    }

    (idx1_sorted, idx2_sorted, dist_sorted) // Return all three vectors
}
