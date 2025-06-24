use crate::merge::Merge;
use extendr_api::prelude::*;
use extendr_api::Rtype::{Doubles, Integers, Logicals, Strings};

impl Merge {
    pub fn anti(df1: &List, idx1: Vec<usize>) -> Robj {
        // Generate vectors of column names and R objects
        let num_cols: usize = df1.nrows();
        let mut names: Vec<String> = Vec::with_capacity(num_cols);
        let mut combined: Vec<Robj> = Vec::with_capacity(num_cols);

        // Now, let's get indices for unmatched LHS rows
        let lhs_complement: Vec<usize> = (1..(df1.index(1).unwrap().len() + 1))
            .filter(|i| !idx1.contains(i))
            .collect();

        // For the left-hand side, we will first take the matched rows then append unmatched
        // rows. This is necessary to maintain order as we will later make blank RHS rows
        // for each unmatched record.
        for (name, col1) in df1.iter() {
            let lhs_type = col1.rtype();
            let errmsg = format!("Trouble converting {:?} at {name}", lhs_type);

            let lhs_data: Robj = match lhs_type {
                Integers => {
                    let vals = col1
                        .slice(&lhs_complement)
                        .expect(&errmsg)
                        .as_integer_vector()
                        .expect(&errmsg);
                    vals.into_iter().collect_robj()
                }
                Strings => {
                    let vals: Vec<Option<String>> = col1
                        .slice(&lhs_complement)
                        .expect(&errmsg)
                        .as_str_iter()
                        .expect(&errmsg)
                        .map(|x| if x.is_na() { None } else { Some(x.to_string()) })
                        .collect();
                    vals.into_iter().collect_robj()
                }
                Doubles => {
                    let vals = col1
                        .slice(&lhs_complement)
                        .expect(&errmsg)
                        .as_real_vector()
                        .expect(&errmsg);
                    vals.into_iter().collect_robj()
                }
                Logicals => {
                    let vals = col1
                        .slice(&lhs_complement)
                        .expect(&errmsg)
                        .as_logical_vector()
                        .expect(&errmsg);
                    vals.into_iter().collect_robj()
                }
                _ => panic!("Unexpected error while processing data: is the data type supoported?"),
            };
            names.push(name.to_string());
            combined.push(lhs_data);
        }

        let out: Robj = List::from_names_and_values(names, combined)
            .unwrap()
            .as_robj()
            .clone();
        data_frame!(out)
    }
}
