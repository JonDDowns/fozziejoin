use crate::merge::{
    build_distance_columns, build_single_distance_column, combine_robj, pad_column, Merge,
};
use extendr_api::prelude::*;

impl Merge {
    pub fn full(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: &Vec<Vec<f64>>,
        by: List,
    ) -> List {
        let lhs_len = df1.index(1).unwrap().len();
        let rhs_len = df2.index(1).unwrap().len();

        let lhs_complement: Vec<usize> = (1..=lhs_len).filter(|i| !idx1.contains(i)).collect();
        let rhs_complement: Vec<usize> = (1..=rhs_len).filter(|j| !idx2.contains(j)).collect();

        let unmatched_lhs = lhs_complement.len();
        let unmatched_rhs = rhs_complement.len();

        let (mut names, mut combined): (Vec<String>, Vec<Robj>) = df1
            .iter()
            .map(|(name, col)| {
                let matched = col.slice(&idx1).unwrap();
                let unmatched = col.slice(&lhs_complement).unwrap();
                let pad = pad_column(&col, unmatched_rhs);
                let merged =
                    combine_robj(&combine_robj(&matched, &unmatched).unwrap(), &pad).unwrap();
                (format!("{}{}", name, ".x"), merged)
            })
            .unzip();

        for (name, col) in df2.iter() {
            let matched = col.slice(&idx2).unwrap();
            let pad = pad_column(&col, unmatched_lhs);
            let unmatched = col.slice(&rhs_complement).unwrap();
            let merged = combine_robj(&combine_robj(&matched, &pad).unwrap(), &unmatched).unwrap();
            names.push(format!("{}{}", name, ".y"));
            combined.push(merged);
        }

        if let Some(colname) = distance_col {
            let (dist_names, dist_cols) = build_distance_columns(dist, &by, &colname);
            for (vals, name) in dist_cols.into_iter().zip(dist_names) {
                let mut padded = vals.as_real_slice().unwrap().to_vec();
                padded.extend(vec![f64::NAN; unmatched_lhs + unmatched_rhs]);
                names.push(name);
                combined.push(padded.into_robj());
            }
        }

        let out = List::from_names_and_values(names, combined).unwrap();
        out
    }

    pub fn full_single(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: &Vec<f64>,
    ) -> List {
        let lhs_len = df1.index(1).unwrap().len();
        let rhs_len = df2.index(1).unwrap().len();

        let lhs_complement: Vec<usize> = (1..=lhs_len).filter(|i| !idx1.contains(i)).collect();
        let rhs_complement: Vec<usize> = (1..=rhs_len).filter(|j| !idx2.contains(j)).collect();

        let unmatched_lhs = lhs_complement.len();
        let unmatched_rhs = rhs_complement.len();

        let (mut names, mut combined): (Vec<String>, Vec<Robj>) = df1
            .iter()
            .map(|(name, col)| {
                let matched = col.slice(&idx1).unwrap();
                let unmatched = col.slice(&lhs_complement).unwrap();
                let pad = pad_column(&col, unmatched_rhs);
                let merged =
                    combine_robj(&combine_robj(&matched, &unmatched).unwrap(), &pad).unwrap();
                (format!("{}{}", name, ".x"), merged)
            })
            .unzip();

        for (name, col) in df2.iter() {
            let matched = col.slice(&idx2).unwrap();
            let pad = pad_column(&col, unmatched_lhs);
            let unmatched = col.slice(&rhs_complement).unwrap();
            let merged = combine_robj(&combine_robj(&matched, &pad).unwrap(), &unmatched).unwrap();
            names.push(format!("{}{}", name, ".y"));
            combined.push(merged);
        }

        if let Some(colname) = distance_col {
            let (name, vals) = build_single_distance_column(dist, &colname);
            let mut padded = vals.as_real_slice().unwrap().to_vec();
            padded.extend(vec![f64::NAN; unmatched_lhs + unmatched_rhs]);
            names.push(name);
            combined.push(padded.into_robj());
        }

        List::from_names_and_values(names, combined).unwrap()
    }
}
