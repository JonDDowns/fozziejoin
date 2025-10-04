use anyhow::{anyhow, Result};
use core::f64;
use extendr_api::prelude::*;
use itertools::MultiUnzip;
use rayon::prelude::*;
use rayon::ThreadPool;

fn zip_columns(columns: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let n_rows = columns[0].len();
    let n_cols = columns.len();

    (0..n_rows)
        .map(|i| {
            let mut row = Vec::with_capacity(n_cols);
            for col in columns {
                row.push(col[i]);
            }
            row
        })
        .collect::<Vec<_>>()
}

pub fn fuzzy_indices_dist(
    df1: &List,
    df2: &List,
    by: &List,
    method: &str,
    max_distance: f64,
    pool: &ThreadPool,
) -> Result<(Vec<usize>, Vec<usize>, Vec<f64>)> {
    let keys: Vec<(String, String)> = by
        .iter()
        .map(|(left_key, val)| {
            let right_key = val
                .as_string_vector()
                .ok_or_else(|| anyhow!("Missing or invalid right key for '{}'", left_key))?;
            Ok((left_key.to_string(), right_key[0].clone()))
        })
        .collect::<Result<_>>()?;

    let distmetric = DistanceMetric::new(method)?;

    let mut left_vecs = Vec::new();
    let mut right_vecs = Vec::new();

    for (left_key, right_key) in &keys {
        let leftvec = df1
            .dollar(left_key)
            .map_err(|_| anyhow!("Column '{}' not found in df1", left_key))?
            .as_real_vector()
            .ok_or_else(|| anyhow!("Column '{}' in df1 is not numeric", left_key))?;
        left_vecs.push(leftvec);

        let rightvec = df2
            .dollar(right_key)
            .map_err(|_| anyhow!("Column '{}' not found in df2", right_key))?
            .as_real_vector()
            .ok_or_else(|| anyhow!("Column '{}' in df2 is not numeric", right_key))?;
        right_vecs.push(rightvec);
    }

    let left_rows = zip_columns(&left_vecs);
    let right_rows = zip_columns(&right_vecs);

    let (idxs1, idxs2, dists) =
        filtered_distances(&left_rows, &right_rows, max_distance, distmetric, pool)?;

    Ok((idxs1, idxs2, dists))
}

#[derive(Debug, Clone, Copy)]
pub enum DistanceMetric {
    Euclidean,
    Manhattan,
}

impl DistanceMetric {
    pub fn new(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "euclidean" | "euc" => Ok(DistanceMetric::Euclidean),
            "manhattan" | "man" => Ok(DistanceMetric::Manhattan),
            _ => Err(anyhow!("Unknown distance metric: {}", s)),
        }
    }
}

pub fn filtered_distances(
    left: &[Vec<f64>],
    right: &[Vec<f64>],
    threshold: f64,
    metric: DistanceMetric,
    pool: &rayon::ThreadPool,
) -> anyhow::Result<(Vec<usize>, Vec<usize>, Vec<f64>)> {
    pool.install(|| {
        let results: Result<Vec<(usize, usize, f64)>> = left
            .par_iter()
            .enumerate()
            .flat_map_iter(|(i, a)| {
                right.iter().enumerate().filter_map(move |(j, b)| {
                    if a.len() != b.len() {
                        return Some(Err(anyhow!(
                            "Vector length mismatch at left[{}] and right[{}]",
                            i,
                            j
                        )));
                    }

                    let dist = match metric {
                        DistanceMetric::Euclidean => a
                            .iter()
                            .zip(b.iter())
                            .map(|(x, y)| (x - y).powi(2))
                            .sum::<f64>()
                            .sqrt(),
                        DistanceMetric::Manhattan => a
                            .iter()
                            .zip(b.iter())
                            .map(|(x, y)| (x - y).abs())
                            .sum::<f64>(),
                    };

                    if dist <= threshold {
                        Some(Ok((i + 1, j + 1, dist)))
                    } else {
                        None
                    }
                })
            })
            .collect();

        match results {
            Ok(triples) => {
                let (left_indices, right_indices, distances): (Vec<_>, Vec<_>, Vec<_>) =
                    triples.into_iter().multiunzip();
                Ok((left_indices, right_indices, distances))
            }
            Err(e) => Err(e),
        }
    })
}
