use crate::merge::{
    build_distance_columns, build_single_distance_column, combine_robj, pad_column, DistanceData,
    Merge,
};
use extendr_api::prelude::*;
use rustc_hash::FxHashSet;

impl Merge {
    pub fn full(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: DistanceData,
        by: List,
    ) -> List {
        let lhs_len = df1.index(1).unwrap().len();
        let rhs_len = df2.index(1).unwrap().len();

        let lhs_complement: Vec<usize> = (1..=lhs_len).filter(|i| !idx1.contains(i)).collect();
        let rhs_complement: Vec<usize> = (1..=rhs_len).filter(|j| !idx2.contains(j)).collect();

        let unmatched_lhs = lhs_complement.len();
        let unmatched_rhs = rhs_complement.len();

        let df1_names: FxHashSet<&str> = df1.names().unwrap_or_default().into_iter().collect();
        let df2_names: FxHashSet<&str> = df2.names().unwrap_or_default().into_iter().collect();
        let shared: FxHashSet<&str> = df1_names.intersection(&df2_names).cloned().collect();

        let (mut names, mut combined): (Vec<String>, Vec<Robj>) = df1
            .iter()
            .map(|(name, col)| {
                let matched = col.slice(&idx1).unwrap();
                let unmatched = col.slice(&lhs_complement).unwrap();
                let pad = pad_column(&col, unmatched_rhs);
                let merged =
                    combine_robj(&combine_robj(&matched, &unmatched).unwrap(), &pad).unwrap();
                let final_name = if shared.contains(&name) {
                    format!("{}{}", name, ".x")
                } else {
                    name.to_string()
                };
                (final_name, merged)
            })
            .unzip();

        for (name, col) in df2.iter() {
            let matched = col.slice(&idx2).unwrap();
            let pad = pad_column(&col, unmatched_lhs);
            let unmatched = col.slice(&rhs_complement).unwrap();
            let merged = combine_robj(&combine_robj(&matched, &pad).unwrap(), &unmatched).unwrap();
            let final_name = if shared.contains(&name) {
                format!("{}{}", name, ".y")
            } else {
                name.to_string()
            };
            names.push(final_name);
            combined.push(merged);
        }

        if let Some(colname) = distance_col {
            match dist {
                DistanceData::Single(vec) => {
                    let (name, vals) = build_single_distance_column(vec, &colname);
                    let mut padded = vals.as_real_slice().unwrap().to_vec();
                    padded.extend(vec![f64::NAN; unmatched_lhs + unmatched_rhs]);
                    names.push(name);
                    combined.push(padded.into_robj());
                }
                DistanceData::Matrix(mat) => {
                    let (dist_names, dist_cols) = build_distance_columns(mat, &by, &colname);
                    for (vals, name) in dist_cols.into_iter().zip(dist_names) {
                        let mut padded = vals.as_real_slice().unwrap().to_vec();
                        padded.extend(vec![f64::NAN; unmatched_lhs + unmatched_rhs]);
                        names.push(name);
                        combined.push(padded.into_robj());
                    }
                }
            }
        }

        List::from_names_and_values(names, combined).unwrap()
    }
}
