use crate::interval::OverlapType;
use anyhow::{anyhow, Result};
use extendr_api::prelude::*;
use interavl::IntervalTree;
use rayon::prelude::*;
use rayon::ThreadPool;

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

    // Build interval tree from df2
    let mut tree: IntervalTree<i32, Vec<usize>> = IntervalTree::default();
    for (j, (&rs, &re)) in right_start.iter().zip(right_end.iter()).enumerate() {
        let rng = &(rs..(re + 1));
        match tree.get_mut(&rng) {
            Some(vec) => vec.push(j),
            None => {
                tree.insert(rng.clone(), vec![j]);
            }
        }
    }

    pool.install(|| {
        let mut results: Vec<(usize, usize)> = left_start
            .par_iter()
            .zip(left_end.par_iter())
            .enumerate()
            .flat_map_iter(|(i, (&ls, &le))| {
                let query = (ls - maxgap - 1)..((maxgap + le) + 2);

                tree.iter_overlaps(&query)
                    .flat_map(move |(range, jvec)| {
                        let mut idxs = vec![];
                        let rs = range.start;
                        let re = range.end - 1;

                        let gap = if le < rs {
                            rs - le - 1
                        } else if re < ls {
                            ls - re - 1
                        } else {
                            0
                        };

                        let overlap_len = (le.min(re) - ls.max(rs) + 1).max(0);

                        if gap > maxgap || overlap_len < minoverlap {
                            return None;
                        }

                        let semantic_match = match overlap_type {
                            OverlapType::Any => true,
                            OverlapType::Within => ls >= rs - maxgap && le <= re + maxgap,
                            OverlapType::Start => (ls - rs).abs() <= maxgap,
                            OverlapType::End => (le - re).abs() <= maxgap,
                        };

                        if semantic_match {
                            jvec.iter().for_each(|j| idxs.push((i + 1, j + 1)));
                        }
                        Some(idxs)
                    })
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect();
        results.sort_unstable();

        Ok((
            results.iter().map(|(i, _)| *i).collect(),
            results.iter().map(|(_, j)| *j).collect(),
        ))
    })
}
