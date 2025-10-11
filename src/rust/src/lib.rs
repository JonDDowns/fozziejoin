use anyhow::{anyhow, Result};
use core::f64;
use extendr_api::prelude::*;

pub mod difference;
pub mod distance;
pub mod interval;
pub mod merge;
pub mod regex;
pub mod string;
pub mod utils;

use crate::difference::{difference_join, difference_pairs};
use crate::distance::fuzzy_indices_dist;
use crate::interval::integer::fuzzy_indices_interval_int;
use crate::interval::real::fuzzy_indices_interval_real;
use crate::merge::dispatch_join;
use crate::merge::DistanceData;
use crate::regex::{regex_join, regex_pairs};
use crate::string::string_join;
use crate::utils::get_pool;

/// @title Internal: String Join via Rust
/// @description Internal function. Performs a string-based fuzzy join using Rust backend.
/// @keywords internal
/// @export
#[extendr]
pub fn fozzie_string_join_rs(
    df1: List,
    df2: List,
    by: List,
    method: String,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    q: Option<i32>,
    max_prefix: Option<i32>,
    prefix_weight: Option<f64>,
    nthread: Option<usize>,
) -> Result<List> {
    let result = string_join(
        df1,
        df2,
        by,
        method,
        how,
        max_distance,
        distance_col,
        q,
        max_prefix,
        prefix_weight,
        nthread,
    )
    .map_err(|e| anyhow!("Error in string join: {e}!"))?;
    Ok(result)
}

/// @title Internal: Difference Join via Rust
/// @description Internal function. Performs a difference-based fuzzy join using Rust backend.
/// @keywords internal
/// @export
#[extendr]
pub fn fozzie_difference_join_rs(
    df1: List,
    df2: List,
    by: List,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    nthread: Option<usize>,
) -> Result<List> {
    let pool = get_pool(nthread)?;

    let keys: Vec<(String, String)> = by
        .iter()
        .map(|(left_key, val)| {
            let right_key = val
                .as_string_vector()
                .ok_or_else(|| anyhow!("Missing string vector for key '{}'", left_key))?;
            Ok((left_key.to_string(), right_key[0].clone()))
        })
        .collect::<Result<_>>()?;

    let (mut idxs1, mut idxs2, dists) =
        difference_join(&df1, &df2, keys[0].clone(), max_distance, &pool)
            .map_err(|e| anyhow!("Failed initial difference join: {}", e))?;

    let out: List = if keys.len() == 1 {
        let dists = DistanceData::Single(&dists);
        dispatch_join(
            how.as_str(),
            &df1,
            &df2,
            idxs1,
            idxs2,
            distance_col,
            dists,
            by,
        )
    } else {
        let mut dists = vec![dists];
        for bypair in &keys[1..] {
            let (a, b, c) = difference_pairs(
                &df1,
                &idxs1,
                &df2,
                &idxs2,
                bypair,
                &dists,
                max_distance,
                &pool,
            )
            .map_err(|e| anyhow!("Failed difference_pairs for {:?}: {}", bypair, e))?;
            idxs1 = a;
            idxs2 = b;
            dists = c;
        }
        let dists = DistanceData::Matrix(&dists);
        dispatch_join(
            how.as_str(),
            &df1,
            &df2,
            idxs1,
            idxs2,
            distance_col,
            dists,
            by,
        )
    };

    Ok(out)
}

/// @title Internal: Distance Join via Rust
/// @description Internal function. Performs a distance-based fuzzy join using Rust backend.
/// @keywords internal
/// @export
#[extendr]
pub fn fozzie_distance_join_rs(
    df1: List,
    df2: List,
    by: List,
    method: String,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    nthread: Option<usize>,
) -> Result<List> {
    let pool = get_pool(nthread)?;

    let (idxs1, idxs2, dists) = fuzzy_indices_dist(&df1, &df2, &by, &method, max_distance, &pool)
        .map_err(|e| anyhow!("Error when finding fuzzy matches: {e}"))?;
    let dists = DistanceData::Single(&dists);
    let joined = dispatch_join(
        how.as_str(),
        &df1,
        &df2,
        idxs1,
        idxs2,
        distance_col,
        dists,
        by,
    );
    Ok(joined)
}

/// @title Internal: Interval Join via Rust
/// @description Internal function. Performs an interval-based fuzzy join using Rust backend.
/// @keywords internal
/// @export
#[extendr]
pub fn fozzie_interval_join_rs(
    df1: List,
    df2: List,
    by: List,
    how: String,
    overlap_type: String,
    maxgap: f64,
    minoverlap: f64,
    interval_mode: &str,
    nthread: Option<usize>,
) -> Result<List> {
    let pool = get_pool(nthread)?;

    let (idxs1, idxs2) = match interval_mode {
        "real" => {
            fuzzy_indices_interval_real(&df1, &df2, &by, &overlap_type, maxgap, minoverlap, &pool)
        }
        "int" | "integer" => fuzzy_indices_interval_int(
            &df1,
            &df2,
            &by,
            &overlap_type,
            maxgap as i32,
            minoverlap as i32,
            &pool,
        ),
        _ => panic!("Uhoh!"),
    }
    .map_err(|e| anyhow!("Error when finding fuzzy matches: {e}"))?;
    let empty = vec![];
    let dists = DistanceData::Single(&empty);

    let joined = dispatch_join(how.as_str(), &df1, &df2, idxs1, idxs2, None, dists, by);
    Ok(joined)
}

/// @title Internal: Regex Join via Rust
/// @description Internal function. Performs a regex-based fuzzy join using Rust backend.
/// @keywords internal
/// @export
#[extendr]
pub fn fozzie_regex_join_rs(
    df1: List,
    df2: List,
    by: List,
    how: String,
    ignore_case: bool,
    nthread: Option<usize>,
) -> Result<List> {
    let pool = get_pool(nthread)?;

    let keys: Vec<(String, String)> = by
        .iter()
        .map(|(left_key, val)| {
            let right_key = val
                .as_string_vector()
                .ok_or_else(|| anyhow!("Missing string vector for key '{}'", left_key))?;
            Ok((left_key.to_string(), right_key[0].clone()))
        })
        .collect::<Result<_>>()?;

    let (mut idxs1, mut idxs2) = regex_join(&df1, &df2, keys[0].clone(), ignore_case, &pool)
        .map_err(|e| anyhow!("Failed initial regex join: {}", e))?;

    let out: List = if keys.len() == 1 {
        let dists: Vec<f64> = Vec::new();
        let dists = DistanceData::Single(&dists);
        dispatch_join(how.as_str(), &df1, &df2, idxs1, idxs2, None, dists, by)
    } else {
        for bypair in &keys[1..] {
            let (a, b) = regex_pairs(&df1, &idxs1, &df2, &idxs2, bypair, ignore_case, &pool)
                .map_err(|e| anyhow!("Failed difference_pairs for {:?}: {}", bypair, e))?;
            idxs1 = a;
            idxs2 = b;
        }
        let dists: Vec<Vec<f64>> = Vec::new();
        let dists = DistanceData::Matrix(&dists);
        dispatch_join(how.as_str(), &df1, &df2, idxs1, idxs2, None, dists, by)
    };

    Ok(out)
}

/// @title Get number of threads in global thread pool
/// @description Returns default rayon number of threads
/// @keywords internal
/// @export
#[extendr]
fn get_nthread_default() -> usize {
    rayon::current_num_threads()
}

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_string_join_rs;
    fn fozzie_difference_join_rs;
    fn fozzie_distance_join_rs;
    fn fozzie_interval_join_rs;
    fn fozzie_regex_join_rs;
    fn get_nthread_default;
}
