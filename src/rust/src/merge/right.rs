use crate::merge::Merge;
use extendr_api::prelude::*;
use extendr_api::Rtype::{Doubles, Integers, Logicals, Strings};

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

        // Now, let's get indices for unmatched RHS rows
        let rhs_complement: Vec<usize> = (1..(df2.index(1).unwrap().len() + 1))
            .filter(|i| !idx2.contains(i))
            .collect();

        // Now, for the left-hand side, we first take all matched rows.
        // Then, we will append NA values for all unmatched rows from LHS.
        for (name, col1) in df1.iter() {
            let lhs_type = col1.rtype();
            let errmsg = format!("Trouble converting {:?} at {name}", lhs_type);

            // Dates need special handling. Set indicators.
            let mut is_date: bool = false;

            // Loop through classes, determine if any special cases are present
            let cls = col1.class();
            if let Some(classes) = cls {
                let classes: Vec<&str> = classes.collect();
                for class in classes.iter() {
                    match *class {
                        "Date" => is_date = true,
                        _ => continue,
                    }
                }
            }

            // Extract factor levels if necessary
            let levels = if let Some(attributes) = col1.get_attrib("levels") {
                attributes.as_string_vector()
            } else {
                None
            };

            let rhs_data: Robj = match lhs_type {
                Integers => {
                    // Matched records
                    let vals1: Vec<Option<i32>> = col1
                        .slice(&idx1)
                        .expect(&errmsg)
                        .as_integer_vector()
                        .expect(&errmsg)
                        .into_iter()
                        .map(|x| Some(x))
                        .collect();

                    // Placeholders for everything else
                    let vals2: Vec<Option<i32>> = vec![None; rhs_complement.len()];

                    // Create combined object
                    let mut merged = vals1.into_iter().chain(vals2.into_iter()).collect_robj();

                    // If necessary, re-apply factor levels
                    if let Some(lev) = levels {
                        merged.set_class(&["factor"]).unwrap();
                        merged.set_attrib("levels", lev.into_robj()).unwrap();
                    }

                    merged
                }
                Strings => {
                    // Matched records
                    let vals1: Vec<Option<String>> = col1
                        .slice(&idx1)
                        .expect(&errmsg)
                        .as_str_iter()
                        .expect(&errmsg)
                        .map(|x| if x.is_na() { None } else { Some(x.to_string()) })
                        .collect();
                    let vals2: Vec<Option<String>> = vec![None; rhs_complement.len()];
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                Doubles => {
                    // Matched records
                    let vals1: Vec<Option<f64>> = col1
                        .slice(&idx1)
                        .expect(&errmsg)
                        .as_real_vector()
                        .expect(&errmsg)
                        .into_iter()
                        .map(|x| Some(x))
                        .collect();

                    // Placeholders for everything else
                    let vals2: Vec<Option<f64>> = vec![None; rhs_complement.len()];

                    // Create combined object
                    let mut merged = vals1.into_iter().chain(vals2.into_iter()).collect_robj();

                    // If it's a date, set the class again
                    if is_date {
                        merged
                            .set_class(&["Date"])
                            .expect(&format!("Trouble converting {name} to date"));
                    }
                    merged
                }
                Logicals => {
                    // Matched records
                    let vals1: Vec<Rbool> = col1
                        .slice(&idx1)
                        .expect(&errmsg)
                        .as_logical_vector()
                        .expect(&errmsg);

                    // Placeholders for everything else
                    let vals2: Vec<Rbool> = vec![NA_LOGICAL; rhs_complement.len()];

                    // Return final set
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                _ => panic!(
                    "Unexpected error while processing RHS data: is the data type supported?"
                ),
            };
            names.push(name.to_string() + ".x");
            combined.push(rhs_data);
        }

        // For the right-hand side, we will first take the matched rows then append unmatched
        // rows. NA's have already been filled in for the LHS's unmatched rows
        for (name, col2) in df2.iter() {
            let rhs_type = col2.rtype();
            let errmsg = format!("Trouble converting {:?} at {name}", rhs_type);

            // Dates need special handling. Set indicators.
            let mut is_date: bool = false;

            // Loop through classes, determine if any special cases are present
            let cls = col2.class();
            if let Some(classes) = cls {
                let classes: Vec<&str> = classes.collect();
                for class in classes.iter() {
                    match *class {
                        "Date" => is_date = true,
                        _ => continue,
                    }
                }
            }

            // Extract factor levels if necessary
            let levels = if let Some(attributes) = col2.get_attrib("levels") {
                attributes.as_string_vector()
            } else {
                None
            };
            let rhs_data: Robj = match rhs_type {
                Integers => {
                    // Matches
                    let vals1 = col2
                        .slice(&idx2)
                        .expect(&errmsg)
                        .as_integer_vector()
                        .expect(&errmsg);

                    // Everything else
                    let vals2 = col2
                        .slice(&rhs_complement)
                        .expect(&errmsg)
                        .as_integer_vector()
                        .expect(&errmsg);

                    // Create combined object
                    let mut merged = vals1.into_iter().chain(vals2.into_iter()).collect_robj();

                    // If necessary, re-apply factor levels
                    if let Some(lev) = levels {
                        merged.set_class(&["factor"]).unwrap();
                        merged.set_attrib("levels", lev.into_robj()).unwrap();
                    }

                    merged
                }
                Strings => {
                    // Matches
                    let vals1: Vec<Option<String>> = col2
                        .slice(&idx2)
                        .expect(&errmsg)
                        .as_str_iter()
                        .expect(&errmsg)
                        .map(|x| if x.is_na() { None } else { Some(x.to_string()) })
                        .collect();

                    // Everything else
                    let vals2: Vec<Option<String>> = col2
                        .slice(&rhs_complement)
                        .expect(&errmsg)
                        .as_str_iter()
                        .expect(&errmsg)
                        .map(|x| if x.is_na() { None } else { Some(x.to_string()) })
                        .collect();
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                Doubles => {
                    // Matches
                    let vals1 = col2
                        .slice(&idx2)
                        .expect(&errmsg)
                        .as_real_vector()
                        .expect(&errmsg);
                    // Everything else
                    let vals2 = col2
                        .slice(&rhs_complement)
                        .expect(&errmsg)
                        .as_real_vector()
                        .expect(&errmsg);
                    // Create combined object
                    let mut merged = vals1.into_iter().chain(vals2.into_iter()).collect_robj();

                    // If it's a date, set the class again
                    if is_date {
                        merged
                            .set_class(&["Date"])
                            .expect(&format!("Trouble converting {name} to date"));
                    }
                    merged
                }
                Logicals => {
                    // Matches
                    let vals1 = col2
                        .slice(&idx2)
                        .expect(&errmsg)
                        .as_logical_vector()
                        .expect(&errmsg);
                    // Everything else
                    let vals2 = col2
                        .slice(&rhs_complement)
                        .expect(&errmsg)
                        .as_logical_vector()
                        .expect(&errmsg);
                    vals1.into_iter().chain(vals2.into_iter()).collect_robj()
                }
                _ => panic!(
                    "Unexpected error while processing LHS data: is the data type supported?"
                ),
            };
            names.push(name.to_string() + ".y");
            combined.push(rhs_data);
        }

        let ndist = dist.len();
        if let Some(colname) = distance_col {
            dist.iter().zip(by.iter()).for_each(|(x, (y, z))| {
                let cname = match ndist {
                    1 => colname.clone(),
                    _ => colname.clone() + &format!("_{}_{}", y, z.as_str_vector().expect("hi")[0]),
                };
                names.push(cname);
                let na_vals: Vec<Option<f64>> = vec![None; rhs_complement.len()];
                let mut vals: Vec<Option<f64>> = x.clone();
                vals.extend(na_vals);
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
