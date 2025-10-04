use crate::merge::{
    build_distance_columns, build_single_distance_column, combine_robj, pad_column, DistanceData,
    Merge,
};
use extendr_api::prelude::*;
use rustc_hash::FxHashSet;
impl Merge {
    pub fn left(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: DistanceData,
        by: List,
    ) -> List {
        let lhs_len = df1.index(1).unwrap().len();
        let lhs_complement: Vec<usize> = (1..=lhs_len).filter(|i| !idx1.contains(i)).collect();
        let pad_len = lhs_complement.len();

        let df1_names: FxHashSet<&str> = df1.names().unwrap_or_default().into_iter().collect();
        let df2_names: FxHashSet<&str> = df2.names().unwrap_or_default().into_iter().collect();
        let shared: FxHashSet<&str> = df1_names.intersection(&df2_names).cloned().collect();

        let mut names = Vec::new();
        let mut combined = Vec::new();

        // Left-hand side: matched + unmatched
        for (name, col) in df1.iter() {
            let matched = col.slice(&idx1).unwrap();
            let unmatched = col.slice(&lhs_complement).unwrap();
            let merged = combine_robj(&matched, &unmatched).expect("Failed to combine LHS");
            let final_name = if shared.contains(&name) {
                format!("{}{}", name, ".x")
            } else {
                name.to_string()
            };
            names.push(final_name);
            combined.push(merged);
        }

        // Right-hand side: matched + NA padding
        for (name, col) in df2.iter() {
            let matched = col.slice(&idx2).unwrap();
            let pad = pad_column(&col, pad_len);
            let merged = combine_robj(&matched, &pad).expect("Failed to combine RHS");
            let final_name = if shared.contains(&name) {
                format!("{}{}", name, ".y")
            } else {
                name.to_string()
            };
            names.push(final_name);
            combined.push(merged);
        }

        // Distance column(s): matched + NA padding
        if let Some(colname) = distance_col {
            match dist {
                DistanceData::Single(vec) => {
                    let (name, vals) = build_single_distance_column(vec, &colname);
                    let mut padded = vals.as_real_slice().unwrap().to_vec();
                    padded.extend(vec![f64::NAN; pad_len]);
                    names.push(name);
                    combined.push(padded.into_robj());
                }
                DistanceData::Matrix(mat) => {
                    let (dist_names, dist_cols) = build_distance_columns(mat, &by, &colname);
                    for (vals, name) in dist_cols.into_iter().zip(dist_names) {
                        let mut padded = vals.as_real_slice().unwrap().to_vec();
                        padded.extend(vec![f64::NAN; pad_len]);
                        names.push(name);
                        combined.push(padded.into_robj());
                    }
                }
            }
        }

        List::from_names_and_values(names, combined).unwrap()
    }
}
