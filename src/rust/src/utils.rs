use anyhow::{anyhow, Result};
use extendr_api::prelude::*;
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use rustc_hash::FxHashMap;

pub fn robj_index_map<'a>(df: &'a List, key: &'a str) -> Result<FxHashMap<&'a str, Vec<usize>>> {
    let mut map: FxHashMap<&str, Vec<usize>> = FxHashMap::default();

    df.dollar(key)
        .map_err(|_| anyhow!("Column {key} does not exist or is not string."))?
        .as_str_iter()
        .ok_or_else(|| anyhow!("Column {key} does not exist or is not string."))?
        .enumerate()
        .for_each(|(index, val)| {
            map.entry(val).or_default().push(index + 1);
        });

    Ok(map)
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
) -> Result<FxHashMap<&'a str, (FxHashMap<&'a str, usize>, Vec<usize>)>> {
    let mut qgram_map: FxHashMap<&'a str, (FxHashMap<&'a str, usize>, Vec<usize>)> =
        FxHashMap::default();

    let str_iter = df
        .dollar(key)
        .map_err(|_| anyhow!("Column '{}' not found in dataframe", key))?
        .as_str_iter()
        .ok_or_else(|| anyhow!("Column '{}' is not a string vector", key))?;

    for (index, val) in str_iter.enumerate() {
        let hm: FxHashMap<&str, usize> = get_qgrams(val, q);
        qgram_map
            .entry(val)
            .and_modify(|v| v.1.push(index + 1))
            .or_insert((hm, vec![index + 1]));
    }

    Ok(qgram_map)
}

pub fn get_qgrams(s: &str, q: usize) -> FxHashMap<&str, usize> {
    let mut qgram_map = FxHashMap::default();

    if s.len() < q {
        return qgram_map;
    }

    let mut char_indices = s.char_indices().collect::<Vec<_>>();
    char_indices.push((s.len(), '\0'));

    for i in 0..=char_indices.len().saturating_sub(q + 1) {
        let start = char_indices[i].0;
        let end = char_indices[i + q].0;
        let qgram = &s[start..end];
        *qgram_map.entry(qgram).or_insert(0) += 1;
    }

    qgram_map
}

pub fn get_pool(nthread: Option<usize>) -> Result<ThreadPool> {
    if let Some(nt) = nthread {
        let pool = ThreadPoolBuilder::new()
            .num_threads(nt)
            .build()
            .map_err(|e| anyhow!("{e}"))?;
        Ok(pool)
    } else {
        let pool = rayon::ThreadPoolBuilder::new()
            .build()
            .map_err(|e| anyhow!("{e}"))?;
        Ok(pool)
    }
}

pub fn any_numeric_to_vec64(df: &List, key: &str) -> Result<Vec<f64>> {
    let df_col = df
        .dollar(key)
        .map_err(|_| anyhow!("Column `{}` not found in df1", key))?;

    let df_vals: Vec<f64> = if let Some(v) = df_col.as_real_vector() {
        v.to_vec()
    } else if let Some(v) = df_col.as_integer_vector() {
        v.iter().map(|&x| x as f64).collect()
    } else {
        return Err(anyhow!(
            "Column `{}` in df1 is not numeric (integer or double)",
            key
        ));
    };

    Ok(df_vals)
}
