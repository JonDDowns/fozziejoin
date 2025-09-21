use crate::merge::Merge;
use extendr_api::prelude::*;

impl Merge {
    pub fn anti(
        df1: &List,
        idx1: Vec<usize>, // indices of matched LHS rows
    ) -> Robj {
        // Indices for unmatched LHS rows
        let lhs_len = df1.index(1).unwrap().len();
        let lhs_complement: Vec<usize> = (1..=lhs_len).filter(|i| !idx1.contains(i)).collect();

        let mut names: Vec<String> = Vec::with_capacity(df1.len());
        let mut combined: Vec<Robj> = Vec::with_capacity(df1.len());

        // Only return unmatched LHS rows
        for (name, col1) in df1.iter() {
            let unmatched = col1.slice(&lhs_complement).unwrap();
            names.push(name.to_string());
            combined.push(unmatched);
        }

        let out: Robj = List::from_names_and_values(names, combined)
            .unwrap()
            .as_robj()
            .clone();
        data_frame!(out)
    }
}
