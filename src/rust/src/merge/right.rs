use crate::merge::{combine_robj, Merge};
use extendr_api::prelude::*;

impl Merge {
    pub fn right(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: &Vec<Vec<Option<f64>>>,
        by: List,
    ) -> Robj {
        // Generate vectors of column names and R objects
        let num_cols: usize = df1.nrows() + df2.nrows();
        let mut names: Vec<String> = Vec::with_capacity(num_cols);
        let mut combined: Vec<Robj> = Vec::with_capacity(num_cols);

        // Indices for unmatched RHS rows
        let rhs_len = df2.index(1).unwrap().len();
        let rhs_complement: Vec<usize> = (1..=rhs_len).filter(|i| !idx2.contains(i)).collect();

        // Left-hand side: matched + NA padding
        for (name, col1) in df1.iter() {
            let matched = col1.slice(&idx1).unwrap();
            let pad_len = rhs_complement.len();
            let pad = match col1.rtype() {
                Rtype::Integers => Robj::from(vec![Rint::na(); pad_len]),
                Rtype::Doubles => Robj::from(vec![Rfloat::na(); pad_len]),
                Rtype::Logicals => Robj::from(vec![Rbool::na(); pad_len]),
                Rtype::Strings => Robj::from(vec![Rstr::na(); pad_len]),
                _ => Robj::from(vec![Robj::from(()); pad_len]),
            };
            let merged = combine_robj(&matched, &pad).expect("Failed to combine LHS");
            names.push(name.to_string() + ".x");
            combined.push(merged);
        }

        // Right-hand side: matched + unmatched
        for (name, col2) in df2.iter() {
            let matched = col2.slice(&idx2).unwrap();
            let unmatched = col2.slice(&rhs_complement).unwrap();
            let merged = combine_robj(&matched, &unmatched).expect("Failed to combine RHS");
            names.push(name.to_string() + ".y");
            combined.push(merged);
        }

        // Distance columns: matched + NA padding
        let ndist = dist.len();
        if let Some(colname) = distance_col {
            dist.iter().zip(by.iter()).for_each(|(x, (y, z))| {
                let cname = match ndist {
                    1 => colname.clone(),
                    _ => colname.clone() + &format!("_{}_{}", y, z.as_str_vector().expect("hi")[0]),
                };
                names.push(cname);
                let mut vals = x.clone();
                vals.extend(vec![None; rhs_complement.len()]);
                combined.push(vals.into_robj());
            });
        }

        let out: Robj = List::from_names_and_values(names, combined)
            .unwrap()
            .as_robj()
            .clone();
        data_frame!(out)
    }
}
