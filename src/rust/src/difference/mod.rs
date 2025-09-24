use core::f64;
use rayon::prelude::*;
use rayon::ThreadPool;
use std::collections::HashMap;

pub fn fuzzy_indices_diff(
    vec1: Vec<f64>,
    vec2: Vec<f64>,
    max_distance: f64,
    pool: &ThreadPool,
) -> HashMap<(usize, usize), Vec<Option<f64>>> {
    let indexed_vec1: Vec<(usize, f64)> = vec1.into_iter().enumerate().collect();
    let indexed_vec2: Vec<(usize, f64)> = vec2.into_iter().enumerate().collect();

    let bucket_width = max_distance;
    let buckets: HashMap<i64, Vec<(usize, f64)>> = {
        let mut map: HashMap<i64, Vec<(usize, f64)>> = HashMap::new();
        for (j_idx, y) in indexed_vec2 {
            let bucket = (y / bucket_width).floor() as i64;
            map.entry(bucket).or_default().push((j_idx, y));
        }
        map
    };

    let threshold = max_distance + f64::EPSILON;

    pool.install(|| {
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
                                Some(((i_idx + 1, j_idx + 1), vec![Some(diff)]))
                            } else {
                                None
                            }
                        })
                    })
            })
            .collect::<HashMap<_, _>>() // Safe because keys are guaranteed unique
    })
}
