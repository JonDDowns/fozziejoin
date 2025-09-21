use crate::merge::combine_robj;
use crate::Merge;
use extendr_api::prelude::*;

impl Merge {
    pub fn full(
        df1: &List,
        df2: &List,
        idx1: Vec<usize>,
        idx2: Vec<usize>,
        distance_col: Option<String>,
        dist: &Vec<Vec<Option<f64>>>,
        by: List,
    ) -> Robj {
        let mut names: Vec<String> = Vec::new();
        let mut combined: Vec<Robj> = Vec::new();

        // Compute unmatched indices
        let lhs_len = df1.index(1).unwrap().len();
        let rhs_len = df2.index(1).unwrap().len();

        let lhs_complement: Vec<usize> = (1..=lhs_len).filter(|i| !idx1.contains(i)).collect();
        let rhs_complement: Vec<usize> = (1..=rhs_len).filter(|i| !idx2.contains(i)).collect();

        // Total unmatched rows
        let unmatched_lhs = lhs_complement.len();
        let unmatched_rhs = rhs_complement.len();

        // Left-hand side: matched + unmatched + NA padding for unmatched RHS
        for (name, col1) in df1.iter() {
            let matched = col1.slice(&idx1).unwrap();
            let unmatched = col1.slice(&lhs_complement).unwrap();
            let pad = match col1.rtype() {
                Rtype::Integers => Robj::from(vec![Rint::na(); unmatched_rhs]),
                Rtype::Doubles => Robj::from(vec![Rfloat::na(); unmatched_rhs]),
                Rtype::Logicals => Robj::from(vec![Rbool::na(); unmatched_rhs]),
                Rtype::Strings => Robj::from(vec![Rstr::na(); unmatched_rhs]),
                _ => Robj::from(vec![Robj::from(()); unmatched_rhs]),
            };
            let merged = combine_robj(&combine_robj(&matched, &unmatched).unwrap(), &pad).unwrap();
            names.push(name.to_string() + ".x");
            combined.push(merged);
        }

        // Right-hand side: matched + NA padding for unmatched LHS + unmatched
        for (name, col2) in df2.iter() {
            let matched = col2.slice(&idx2).unwrap();
            let unmatched = col2.slice(&rhs_complement).unwrap();
            let pad = match col2.rtype() {
                Rtype::Integers => Robj::from(vec![Rint::na(); unmatched_lhs]),
                Rtype::Doubles => Robj::from(vec![Rfloat::na(); unmatched_lhs]),
                Rtype::Logicals => Robj::from(vec![Rbool::na(); unmatched_lhs]),
                Rtype::Strings => Robj::from(vec![Rstr::na(); unmatched_lhs]),
                _ => Robj::from(vec![Robj::from(()); unmatched_lhs]),
            };
            let merged = combine_robj(&combine_robj(&matched, &pad).unwrap(), &unmatched).unwrap();
            names.push(name.to_string() + ".y");
            combined.push(merged);
        }

        // Distance columns: matched + NA padding for unmatched LHS + unmatched RHS
        let ndist = dist.len();
        if let Some(colname) = distance_col {
            dist.iter().zip(by.iter()).for_each(|(x, (y, z))| {
                let cname = match ndist {
                    1 => colname.clone(),
                    _ => colname.clone() + &format!("_{}_{}", y, z.as_str_vector().expect("hi")[0]),
                };
                names.push(cname);
                let mut vals = x.clone();
                vals.extend(vec![None; unmatched_lhs + unmatched_rhs]);
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
