use crate::merge::Merge;
use extendr_api::prelude::*;

impl Merge {
    pub fn inner(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: &Vec<Option<f64>>,
    ) -> Robj {
        // Generate vectors of column names and R objects
        let num_cols: usize = df1.ncols() + df2.ncols();
        let mut names: Vec<String> = Vec::with_capacity(num_cols);
        let mut combined: Vec<Robj> = Vec::with_capacity(num_cols);

        // Subset to matched records in left-hand side, push to main list
        for (name, col1) in df1.iter() {
            let vals = col1.slice(&idx1).unwrap();
            names.push(name.to_string() + ".x");
            combined.push(vals);
        }

        // Subset to matched records in right-hand side, push to main list
        for (name, col2) in df2.iter() {
            let vals = col2.slice(&idx2).unwrap();
            names.push(name.to_string() + ".y");
            combined.push(vals);
        }

        if let Some(colname) = distance_col {
            names.push(colname);
            let vals = dist.into_robj();
            combined.push(vals);
        }

        // Final type conversions and output
        let out: Robj = List::from_names_and_values(names, combined)
            .unwrap()
            .as_robj()
            .clone();
        data_frame!(out)
    }
}
