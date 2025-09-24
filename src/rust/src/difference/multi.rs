use crate::utils::{get_pool, transpose_map_fx};
use crate::Merge;
use anyhow::Result;
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
) -> FxHashMap<(usize, usize), Vec<f64>> {
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
                                Some(((i_idx + 1, j_idx + 1), vec![diff]))
                            } else {
                                None
                            }
                        })
                    })
            })
            .collect::<FxHashMap<_, _>>() // Fast collection
    })
}

pub fn difference_multi_join(
    df1: List,
    df2: List,
    by: List,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    nthread: Option<usize>,
) -> Result<Robj> {
    // Running list of all IDXs that have survived
    let mut keep_idxs: FxHashMap<(usize, usize), Vec<f64>> = FxHashMap::default();

    let pool = get_pool(nthread);

    // Begin looping through each supplied set of match keys
    for (match_iter, (left_key, right_key)) in by.iter().enumerate() {
        let rk = &right_key
            .as_str_vector()
            .expect(&format!("Error converting {:?} to string.", right_key))[0];

        let vec1 = df1
            .dollar(left_key)
            .expect("Error extracting {left_key}")
            .as_real_vector()
            .expect("Error extracting {left_key}");
        let vec2 = df2
            .dollar(rk)
            .expect("Error extracting {rk}")
            .as_real_vector()
            .expect("Error extracting {rk}");
        let matchdat = fuzzy_indices_diff(vec1, vec2, max_distance, &pool);

        if match_iter == 0 {
            // On the first iteration, we initialize the idx's to keep
            keep_idxs = matchdat;
        } else {
            // Extract indices from current run
            let idxs: Vec<(usize, usize)> = matchdat.iter().map(|((a, b), _)| (*a, *b)).collect();

            // Anything that did not survive this iteration should be removed from contention
            keep_idxs.retain(|key, _| idxs.contains(key));

            // Add stringdistance result for survivors
            for ((id1, id2), dist) in matchdat {
                if keep_idxs.contains_key(&(id1, id2)) {
                    keep_idxs.get_mut(&(id1, id2)).expect("hm").extend(dist);
                }
            }
        }
    }

    // Reshape output to enable final DF creation
    let (idxs1, idxs2, dists) = transpose_map_fx(keep_idxs);

    // Create the DF
    let out = match how.as_str() {
        "inner" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "anti" => Merge::anti(&df1, idxs1),
        "full" => Merge::full(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        _ => panic!("Join type not supported"),
    };

    // Final result
    return Ok(out);
}
