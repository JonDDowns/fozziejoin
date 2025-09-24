use crate::merge::{build_distance_columns, build_single_distance_column, subset_and_label, Merge};
use extendr_api::prelude::*;
impl Merge {
    pub fn inner(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: &Vec<Vec<f64>>,
        by: List,
    ) -> Robj {
        let (mut names, mut combined): (Vec<String>, Vec<Robj>) = {
            let (n1, c1) = subset_and_label(df1, &idx1, ".x");
            let (n2, c2) = subset_and_label(df2, &idx2, ".y");
            (
                n1.into_iter().chain(n2).collect(),
                c1.into_iter().chain(c2).collect(),
            )
        };

        if let Some(colname) = distance_col {
            let (dist_names, dist_cols) = build_distance_columns(dist, &by, &colname);
            names.extend(dist_names);
            combined.extend(dist_cols);
        }

        let out = List::from_names_and_values(names, combined)
            .unwrap()
            .as_robj()
            .clone();
        data_frame!(out)
    }

    pub fn inner_single(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: &Vec<f64>,
    ) -> Robj {
        let (mut names, mut combined): (Vec<String>, Vec<Robj>) = {
            let (n1, c1) = subset_and_label(df1, &idx1, ".x");
            let (n2, c2) = subset_and_label(df2, &idx2, ".y");
            (
                n1.into_iter().chain(n2).collect(),
                c1.into_iter().chain(c2).collect(),
            )
        };

        if let Some(colname) = distance_col {
            let (name, col) = build_single_distance_column(dist, &colname);
            names.push(name);
            combined.push(col);
        }

        let out = List::from_names_and_values(names, combined)
            .unwrap()
            .as_robj()
            .clone();
        data_frame!(out)
    }
}
