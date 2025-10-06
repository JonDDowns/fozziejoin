use crate::interval::OverlapType;
use anyhow::{anyhow, Result};
use extendr_api::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPool;
use std::sync::Arc;

pub fn fuzzy_indices_interval_int(
    df1: &List,
    df2: &List,
    by: &List,
    overlap_type: &str,
    maxgap: i32,
    minoverlap: i32,
    pool: &ThreadPool,
) -> Result<(Vec<usize>, Vec<usize>)> {
    let keys: Vec<(String, String)> = by
        .iter()
        .map(|(left_key, val)| {
            let right_key = val
                .as_string_vector()
                .ok_or_else(|| anyhow!("Missing or invalid right key for '{}'", left_key))?;
            Ok((left_key.to_string(), right_key[0].clone()))
        })
        .collect::<Result<_>>()?;

    if keys.len() != 2 {
        return Err(anyhow!(
            "Expected exactly two columns for interval matching (start and end)"
        ));
    }

    let (left_start_key, right_start_key) = &keys[0];
    let (left_end_key, right_end_key) = &keys[1];

    let left_start = df1
        .dollar(left_start_key)
        .map_err(|_| anyhow!("Column '{}' not found in df1", left_start_key))?
        .as_integer_vector()
        .ok_or_else(|| anyhow!("Column '{}' in df1 is not integer", left_start_key))?;

    let left_end = df1
        .dollar(left_end_key)
        .map_err(|_| anyhow!("Column '{}' not found in df1", left_end_key))?
        .as_integer_vector()
        .ok_or_else(|| anyhow!("Column '{}' in df1 is not integer", left_end_key))?;

    let right_start = df2
        .dollar(right_start_key)
        .map_err(|_| anyhow!("Column '{}' not found in df2", right_start_key))?
        .as_integer_vector()
        .ok_or_else(|| anyhow!("Column '{}' in df2 is not integer", right_start_key))?;

    let right_end = df2
        .dollar(right_end_key)
        .map_err(|_| anyhow!("Column '{}' not found in df2", right_end_key))?
        .as_integer_vector()
        .ok_or_else(|| anyhow!("Column '{}' in df2 is not integer", right_end_key))?;

    if left_start.len() != left_end.len() || right_start.len() != right_end.len() {
        return Err(anyhow!("Start and end columns must have equal lengths"));
    }

    for (i, (&start, &end)) in left_start.iter().zip(left_end.iter()).enumerate() {
        if start > end {
            return Err(anyhow!(
                "Invalid interval in df1 at row {}: start > end",
                i + 1
            ));
        }
    }

    for (j, (&start, &end)) in right_start.iter().zip(right_end.iter()).enumerate() {
        if start > end {
            return Err(anyhow!(
                "Invalid interval in df2 at row {}: start > end",
                j + 1
            ));
        }
    }

    let overlap_type = OverlapType::new(overlap_type)?;

    let right_intervals: Arc<Vec<(i32, i32)>> = Arc::new(
        right_start
            .iter()
            .zip(right_end.iter())
            .map(|(&s, &e)| (s, e))
            .collect(),
    );

    pool.install(|| {
        let results: Vec<(usize, usize)> = left_start
            .par_iter()
            .zip(left_end.par_iter())
            .enumerate()
            .flat_map_iter(|(i, (ls, le))| {
                right_intervals
                    .iter()
                    .enumerate()
                    .filter_map(move |(j, (rs, re))| {
                        let gap = if le < rs {
                            rs - le - 1
                        } else if re < ls {
                            ls - re - 1
                        } else {
                            0
                        };

                        let overlap_len = (le.min(re) - ls.max(rs)).max(0);

                        let overlaps = match overlap_type {
                            OverlapType::Any => ls <= re && le >= rs,
                            OverlapType::Within => ls <= rs && re <= le,
                            OverlapType::Start => ls == rs,
                            OverlapType::End => le == re,
                        };

                        if overlaps && gap <= maxgap && overlap_len >= minoverlap {
                            Some((i + 1, j + 1))
                        } else {
                            None
                        }
                    })
            })
            .collect();

        Ok((
            results.iter().map(|(i, _)| *i).collect(),
            results.iter().map(|(_, j)| *j).collect(),
        ))
    })
}
