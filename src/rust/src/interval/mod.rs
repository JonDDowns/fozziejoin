use rayon::prelude::*;
use rayon::ThreadPool;
use std::cmp::Ordering;
use std::sync::Arc;

pub fn fuzzy_indices_interval(
    indexed_vec1: Vec<(usize, f64, f64)>,
    indexed_vec2: Vec<(usize, f64, f64)>,
    max_gap: f64,
    pool: &ThreadPool,
) -> Vec<(usize, usize, Option<f64>)> {
    let vec2_ref = Arc::new(indexed_vec2);

    pool.install(|| {
        indexed_vec1
            .par_iter()
            .flat_map_iter(|&(i_idx, a_start, a_end)| {
                let lower_bound = a_start - max_gap;
                let upper_bound = a_end + max_gap;

                // Binary search for first b_start > upper_bound
                let end_idx = vec2_ref
                    .binary_search_by(|&(_, b_start, _)| {
                        if b_start <= upper_bound {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    })
                    .unwrap_or_else(|x| x);

                vec2_ref[..end_idx]
                    .iter()
                    .filter_map(move |&(j_idx, b_start, b_end)| {
                        if b_end < lower_bound {
                            return None;
                        }

                        let overlap = a_start <= b_end && b_start <= a_end;
                        let gap = if a_end < b_start {
                            b_start - a_end
                        } else if b_end < a_start {
                            a_start - b_end
                        } else {
                            0.0
                        };

                        if overlap {
                            Some((i_idx + 1, j_idx + 1, None))
                        } else if gap <= max_gap {
                            Some((i_idx + 1, j_idx + 1, Some(gap)))
                        } else {
                            None
                        }
                    })
            })
            .collect()
    })
}
