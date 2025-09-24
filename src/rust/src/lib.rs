use core::f64;
use extendr_api::prelude::*;

pub mod difference;
pub mod merge;
pub mod string;
pub mod utils;

use crate::difference::{multi::difference_multi_join, single::difference_single_join};
use crate::string::{string_multi_join, string_single_join};

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
    let n_by = by.len();
    match n_by {
        1 => string_single_join(
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
        ),
        _ => string_multi_join(
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
        ),
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
) -> Robj {
    let n_by = by.len();
    match n_by {
        1 => difference_single_join(df1, df2, by, how, max_distance, distance_col, nthread),
        _ => difference_multi_join(df1, df2, by, how, max_distance, distance_col, nthread),
    }
}

// Export the function to R
extendr_module! {
    mod fozziejoin;
    fn fozzie_string_join_rs;
    fn fozzie_difference_join_rs;
}
