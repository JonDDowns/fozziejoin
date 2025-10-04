use crate::merge::{
    build_distance_columns, build_single_distance_column, subset_and_label, DistanceData, Merge,
};
use extendr_api::prelude::*;
use rustc_hash::FxHashSet;

pub fn merge_and_label_with_suffix(
    df1: &List,
    idx1: &[usize],
    df2: &List,
    idx2: &[usize],
) -> (Vec<String>, Vec<Robj>) {
    let (n1_raw, c1) = subset_and_label(df1, idx1);
    let (n2_raw, c2) = subset_and_label(df2, idx2);

    let n1_set: FxHashSet<String> = n1_raw.iter().cloned().collect();
    let n2_set: FxHashSet<String> = n2_raw.iter().cloned().collect();
    let overlap: FxHashSet<String> = n1_set.intersection(&n2_set).cloned().collect();

    let n1 = n1_raw
        .into_iter()
        .map(|name| {
            if overlap.contains(&name) {
                format!("{}{}", name, ".x")
            } else {
                name
            }
        })
        .collect::<Vec<_>>();

    let n2 = n2_raw
        .into_iter()
        .map(|name| {
            if overlap.contains(&name) {
                format!("{}{}", name, ".y")
            } else {
                name
            }
        })
        .collect::<Vec<_>>();

    (
        n1.into_iter().chain(n2).collect(),
        c1.into_iter().chain(c2).collect(),
    )
}

impl Merge {
    pub fn inner(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: DistanceData,
        by: List,
    ) -> List {
        let (mut names, mut values) = merge_and_label_with_suffix(&df1, &idx1, &df2, &idx2);

        if let Some(colname) = distance_col {
            match dist {
                DistanceData::Single(vec) => {
                    let (name, col) = build_single_distance_column(vec, &colname);
                    names.push(name);
                    values.push(col);
                }
                DistanceData::Matrix(mat) => {
                    let (dist_names, dist_cols) = build_distance_columns(mat, &by, &colname);
                    names.extend(dist_names);
                    values.extend(dist_cols);
                }
            }
        }

        List::from_names_and_values(names, values).unwrap()
    }
}
