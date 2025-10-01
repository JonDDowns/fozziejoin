use crate::utils::get_pool;
use anyhow::{anyhow, Result};
use core::f64;
use extendr_api::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPool;
use rustc_hash::FxHashMap;

fn fuzzy_indices_diff(
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
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|(i, j, d)| {
                lhs_indices.push(i);
                rhs_indices.push(j);
                distances.push(d);
            });

        (lhs_indices, rhs_indices, distances)
    })
}

pub fn difference_join(
    df1: &List,
    df2: &List,
    by: (String, String),
    max_distance: f64,
    nthread: Option<usize>,
) -> Result<(Vec<usize>, Vec<usize>, Vec<f64>)> {
    let pool = get_pool(nthread);
    let lk = by.0.as_str();
    let rk = by.1.as_str();

    let left_col = df1
        .dollar(lk)
        .map_err(|_| anyhow!("Column `{}` not found in df1", lk))?;

    let vec1 = left_col
        .as_real_vector()
        .ok_or_else(|| anyhow!("Column `{}` in df1 is not a numeric vector", lk))?;

    let right_col = df2
        .dollar(rk)
        .map_err(|_| anyhow!("Column `{}` not found in df2", rk))?;

    let vec2 = right_col
        .as_real_vector()
        .ok_or_else(|| anyhow!("Column `{}` in df2 is not a numeric vector", rk))?;

    let (idxs1, idxs2, dists) = fuzzy_indices_diff(vec1, vec2, max_distance, &pool);
    Ok((idxs1, idxs2, dists))
}

pub fn difference_filter(
    df1: &List,
    idxs1: &Vec<usize>,
    df2: &List,
    idxs2: &Vec<usize>,
    by: &(String, String),
    dists: &Vec<Vec<f64>>,
    max_distance: f64,
) -> Result<(Vec<usize>, Vec<usize>, Vec<Vec<f64>>)> {
    let lk = by.0.as_str();
    let rk = by.1.as_str();

    let vec1: Vec<f64> = df1
        .dollar(lk)
        .expect("lul")
        .slice(idxs1)
        .expect("ruhroh")
        .as_real_vector()
        .expect("ohmy");

    let vec2: Vec<f64> = df2
        .dollar(rk)
        .expect("lul")
        .slice(idxs2)
        .expect("ruhroh")
        .as_real_vector()
        .expect("ohmy");

    let threshold = max_distance + f64::EPSILON;

    let (idxs0, newdist): (Vec<usize>, Vec<f64>) = vec1
        .iter()
        .zip(vec2)
        .enumerate()
        .filter_map(|(i, (left, right))| {
            if left.is_na() || right.is_na() {
                return None;
            }
            let diff = (left - right).abs();
            if diff <= threshold {
                Some((i, diff))
            } else {
                None
            }
        })
        .unzip();

    let (idxs1b, idxs2b) = { idxs0.iter().map(|&i| (idxs1[i], idxs2[i])).unzip() };

    let mut dists_out = vec![];
    for distvec in dists {
        let tmp: Vec<f64> = idxs0.iter().map(|&i| distvec[i]).collect();
        dists_out.push(tmp);
    }
    dists_out.push(newdist);
    Ok((idxs1b, idxs2b, dists_out))
}
