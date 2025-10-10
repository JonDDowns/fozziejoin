use crate::interval::OverlapType;
use crate::utils::any_numeric_to_vec64;
use anyhow::{anyhow, Result};
use extendr_api::prelude::*;
use interavl::IntervalTree;
use ordered_float::OrderedFloat;
use rayon::prelude::*;
use rayon::ThreadPool;

pub fn fuzzy_indices_interval_real(
    df1: &List,
    df2: &List,
    by: &List,
    overlap_type: &str,
    maxgap: f64,
    minoverlap: f64,
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

    let left_start = any_numeric_to_vec64(df1, left_start_key)?;
    let left_end = any_numeric_to_vec64(df1, left_end_key)?;
    let right_start = any_numeric_to_vec64(df2, right_start_key)?;
    let right_end = any_numeric_to_vec64(df2, right_end_key)?;

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

    // Build interval tree from df2 using OrderedFloat
    let mut tree: IntervalTree<OrderedFloat<f64>, Vec<usize>> = IntervalTree::default();
    for (j, (&rs, &re)) in right_start.iter().zip(right_end.iter()).enumerate() {
        let rng = &(OrderedFloat(rs)..OrderedFloat(re));
        match tree.get_mut(&rng) {
            Some(vec) => vec.push(j),
            None => {
                tree.insert(rng.clone(), vec![j]);
            }
        }
    }

    let epsilon = 1e-6;
    pool.install(|| {
        let results: Vec<(usize, usize)> = left_start
            .par_iter()
            .zip(left_end.par_iter())
            .enumerate()
            .flat_map_iter(|(i, (&ls, &le))| {
                let query_start = OrderedFloat(ls - maxgap - epsilon);
                let query_end = OrderedFloat(le + maxgap + epsilon);
                let query = query_start..query_end;

                tree.iter_overlaps(&query)
                    .flat_map(move |(range, jvec)| {
                        let mut idxs = vec![];

                        let rs = range.start.0;
                        let re = range.end.0;

                        let gap = if le < rs {
                            rs - le
                        } else if re < ls {
                            ls - re
                        } else {
                            0.0
                        };

                        let overlap_len = (le.min(re) - ls.max(rs)).max(0.0);

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

        Ok((
            results.iter().map(|(i, _)| *i).collect(),
            results.iter().map(|(_, j)| *j).collect(),
        ))
    })
}
