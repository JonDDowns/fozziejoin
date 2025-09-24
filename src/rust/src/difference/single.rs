use crate::utils::get_pool;
use crate::Merge;
use anyhow::{anyhow, Result};
use core::f64;
use extendr_api::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPool;
use rustc_hash::FxHashMap;

fn fuzzy_indices_single_diff(
    vec1: Vec<f64>,
    vec2: Vec<f64>,
    max_distance: f64,
    pool: &ThreadPool,
) -> (Vec<usize>, Vec<usize>, Vec<f64>) {
    let indexed_vec1: Vec<(usize, f64)> = vec1.into_iter().enumerate().collect();
    let indexed_vec2: Vec<(usize, f64)> = vec2.into_iter().enumerate().collect();

    let bucket_width = max_distance;
    let buckets: FxHashMap<i64, Vec<(usize, f64)>> = {
        let mut map: FxHashMap<i64, Vec<(usize, f64)>> = FxHashMap::default();
        for (j_idx, y) in indexed_vec2 {
            let bucket = (y / bucket_width).floor() as i64;
            map.entry(bucket).or_default().push((j_idx, y));
        }
        map
    };

    let threshold = max_distance + f64::EPSILON;

    pool.install(|| {
        let mut lhs_indices = Vec::new();
        let mut rhs_indices = Vec::new();
        let mut distances = Vec::new();

        indexed_vec1
            .par_iter()
            .flat_map_iter(|&(i_idx, x)| {
                let center = (x / bucket_width).floor() as i64;

                [center - 1, center, center + 1]
                    .into_iter()
                    .filter_map(|b| buckets.get(&b))
                    .flat_map(move |bucket| {
                        bucket.iter().filter_map(move |&(j_idx, y)| {
                            let diff = (x - y).abs();
                            if diff <= threshold {
                                Some((i_idx + 1, j_idx + 1, diff))
                            } else {
                                None
                            }
                        })
                    })
            })
            .collect::<Vec<_>>() // Collect to avoid concurrent mutation
            .into_iter()
            .for_each(|(i, j, d)| {
                lhs_indices.push(i);
                rhs_indices.push(j);
                distances.push(d);
            });

        (lhs_indices, rhs_indices, distances)
    })
}

pub fn difference_single_join(
    df1: List,
    df2: List,
    by: List,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    nthread: Option<usize>,
) -> Result<Robj> {
    let pool = get_pool(nthread);

    if by.len() != 1 {
        return Err(anyhow!(
            "Expected exactly one pair of match keys in `by`, found {}",
            by.len()
        ));
    }

    let (left_key, right_key) = by
        .iter()
        .next()
        .ok_or_else(|| anyhow!("No `by` arguments provided."))?;

    let rk_vec = right_key
        .as_str_vector()
        .ok_or_else(|| anyhow!("Right key {:?} is not a string vector", right_key))?;

    let rk = rk_vec
        .get(0)
        .ok_or_else(|| anyhow!("Right key vector is empty"))?;

    let left_col = df1
        .dollar(left_key)
        .map_err(|_| anyhow!("Column `{}` not found in df1", left_key))?;

    let vec1 = left_col
        .as_real_vector()
        .ok_or_else(|| anyhow!("Column `{}` in df1 is not a numeric vector", left_key))?;

    let right_col = df2
        .dollar(rk)
        .map_err(|_| anyhow!("Column `{}` not found in df2", rk))?;

    let vec2 = right_col
        .as_real_vector()
        .ok_or_else(|| anyhow!("Column `{}` in df2 is not a numeric vector", rk))?;

    let (idxs1, idxs2, dists) = fuzzy_indices_single_diff(vec1, vec2, max_distance, &pool);

    let out = match how.as_str() {
        "inner" => Merge::inner_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "left" => Merge::left_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "right" => Merge::right_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "anti" => Merge::anti(&df1, idxs1),
        "full" => Merge::full_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        _ => return Err(anyhow!("Join type `{}` not supported", how)),
    };

    Ok(out)
}
