use crate::merge::Merge;
use extendr_api::prelude::*;
use extendr_api::Rtype::{Doubles, Integers, Logicals, Strings};

impl Merge {
    pub fn left(df1: &List, df2: &List, idx1: Vec<usize>, idx2: Vec<usize>) -> Robj {
        // Generate vectors of column names and R objects
        let num_cols: usize = df1.nrows() + df2.nrows();
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
                    // Matches
                    let vals1 = col1
                        .slice(&idx1)
                        .expect(&errmsg)
                        .as_integer_vector()
                        .expect(&errmsg);
                    // Everything else
                    let vals2 = col1
                        .slice(&lhs_complement)
                        .expect(&errmsg)
                        .as_integer_vector()
                        .expect(&errmsg);
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                Strings => {
                    // Matches
                    let vals1 = col1
                        .slice(&idx1)
                        .expect(&errmsg)
                        .as_string_vector()
                        .expect(&errmsg);
                    // Everything else
                    let vals2 = col1
                        .slice(&lhs_complement)
                        .expect(&errmsg)
                        .as_string_vector()
                        .expect(&errmsg);
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                Doubles => {
                    // Matches
                    let vals1 = col1
                        .slice(&idx1)
                        .expect(&errmsg)
                        .as_real_vector()
                        .expect(&errmsg);
                    // Everything else
                    let vals2 = col1
                        .slice(&lhs_complement)
                        .expect(&errmsg)
                        .as_real_vector()
                        .expect(&errmsg);
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                Logicals => {
                    // Matches
                    let vals1 = col1
                        .slice(&idx1)
                        .expect(&errmsg)
                        .as_logical_vector()
                        .expect(&errmsg);
                    // Everything else
                    let vals2 = col1
                        .slice(&lhs_complement)
                        .expect(&errmsg)
                        .as_logical_vector()
                        .expect(&errmsg);
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                _ => panic!(
                    "Unexpected error while processing LHS data: is the data type supoported?"
                ),
            };
            names.push(name.to_string() + ".x");
            combined.push(lhs_data);
        }

        // Now, for the right-hand side, we first take all matched rows.
        // Then, we will append NA values for all unmatched rows from LHS.
        for (name, col2) in df2.iter() {
            let rhs_type = col2.rtype();
            let errmsg = format!("Trouble converting {:?} at {name}", rhs_type);
            let rhs_data: Robj = match rhs_type {
                Integers => {
                    // Matched records
                    let vals1: Vec<Option<i32>> = col2
                        .slice(&idx2)
                        .expect(&errmsg)
                        .as_integer_vector()
                        .expect(&errmsg)
                        .into_iter()
                        .map(|x| Some(x))
                        .collect();

                    // Placeholders for everything else
                    let vals2: Vec<Option<i32>> = vec![None; lhs_complement.len()];

                    // Return final set
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                Strings => {
                    // Matched records
                    let vals1: Vec<Option<String>> = col2
                        .slice(&idx2)
                        .expect(&errmsg)
                        .as_string_vector()
                        .expect(&errmsg)
                        .into_iter()
                        .map(|x| Some(x))
                        .collect();
                    let vals2: Vec<Option<String>> = vec![None; lhs_complement.len()];
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                Doubles => {
                    // Matched records
                    let vals1: Vec<Option<f64>> = col2
                        .slice(&idx2)
                        .expect(&errmsg)
                        .as_real_vector()
                        .expect(&errmsg)
                        .into_iter()
                        .map(|x| Some(x))
                        .collect();

                    // Placeholders for everything else
                    let vals2: Vec<Option<f64>> = vec![None; lhs_complement.len()];

                    // Return final set
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                Logicals => {
                    // Matched records
                    let vals1: Vec<Rbool> = col2
                        .slice(&idx2)
                        .expect(&errmsg)
                        .as_logical_vector()
                        .expect(&errmsg);

                    // Placeholders for everything else
                    let vals2: Robj = r!(List::from_values(vec![
                        r!(NA_LOGICAL);
                        lhs_complement.len()
                    ]));
                    let vals2: Vec<Rbool> = vals2.as_logical_vector().expect(&errmsg);

                    // Return final set
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                _ => panic!(
                    "Unexpected error while processing RHS data: is the data type supoported?"
                ),
            };
            names.push(name.to_string() + ".y");
            combined.push(rhs_data);
        }
        let out: Robj = List::from_names_and_values(names, combined)
            .unwrap()
            .as_robj()
            .clone();
        data_frame!(out)
    }
}
