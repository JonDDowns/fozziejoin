use extendr_api::prelude::*;
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use rustc_hash::FxHashMap;
use std::collections::HashMap;

pub fn robj_index_map<'a>(df: &'a List, key: &'a str) -> FxHashMap<&'a str, Vec<usize>> {
    let mut map: FxHashMap<&str, Vec<usize>> = FxHashMap::default();

    df.dollar(key)
        .expect(&format!("Column {key} does not exist or is not string."))
        .as_str_iter()
        .expect(&format!("Column {key} does not exist or is not string."))
        .enumerate()
        .for_each(|(index, val)| {
            map.entry(val).or_default().push(index + 1);
        });

    map
}

pub fn transpose_map_fx(
    data: FxHashMap<(usize, usize), Vec<f64>>,
) -> (Vec<usize>, Vec<usize>, Vec<Vec<f64>>) {
    // Convert the HashMap into a sorted Vec by key
    let mut sorted_entries: Vec<((usize, usize), Vec<f64>)> = data.into_iter().collect();
    sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

    // Initialize our 3 output vectors
    let mut keys1 = Vec::new();
    let mut keys2 = Vec::new();
    let mut transposed_values: Vec<Vec<f64>> = Vec::new();

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

pub fn get_pool(nthread: Option<usize>) -> ThreadPool {
    if let Some(nt) = nthread {
        ThreadPoolBuilder::new()
            .num_threads(nt)
            .build()
            .expect("Failed to build custom thread pool")
    } else {
        rayon::ThreadPoolBuilder::new()
            .build()
            .expect("Failed to build default thread pool")
    }
}
