use crate::utils::get_pool;
use core::f64;
use extendr_api::prelude::*;

pub mod difference;
pub mod merge;
pub mod string;
pub mod utils;

use crate::difference::{difference_join, difference_pairs};
use crate::string::string_join;

use merge::Merge;

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
) -> Robj {
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
    );
    match result {
        Ok(obj) => obj,
        Err(e) => {
            rprintln!("Error in fozzie_string_join_rs: {}", e);
            Robj::from(format!("Error: {}", e))
        }
    }
}

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
) -> List {
    let pool = get_pool(nthread);

    let keys: Vec<(String, String)> = by
        .iter()
        .map(|(left_key, val)| {
            let right_key = val.as_string_vector().expect("lul");
            (left_key.to_string(), right_key[0].clone())
        })
        .collect();

    let (mut idxs1, mut idxs2, dists): (Vec<usize>, Vec<usize>, Vec<f64>) =
        difference_join(&df1, &df2, keys[0].clone(), max_distance, &pool).expect("lul");

    let out: List = if keys.len() == 1 {
        let joined = match how.as_str() {
            "inner" => Merge::inner_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
            "left" => Merge::left_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
            "right" => Merge::right_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
            "anti" => Merge::anti(&df1, idxs1),
            "full" => Merge::full_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
            _ => panic!("Problem with join logic!"),
        };
        joined
    } else {
        let mut dists = vec![dists];
        for bypair in keys[1..].iter() {
            (idxs1, idxs2, dists) = difference_pairs(
                &df1,
                &idxs1,
                &df2,
                &idxs2,
                &bypair,
                &dists,
                max_distance,
                &pool,
            )
            .expect("ruhoh");
        }
        let joined = match how.as_str() {
            "inner" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
            "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
            "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
            "anti" => Merge::anti(&df1, idxs1),
            "full" => Merge::full(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
            _ => panic!("I got 99 problems and a join is one"),
        };
        joined
    };
    out
}

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_string_join_rs;
    fn fozzie_difference_join_rs;
}
