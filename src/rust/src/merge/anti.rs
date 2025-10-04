use crate::merge::{subset_and_label, Merge};
use extendr_api::prelude::*;

impl Merge {
    pub fn anti(df1: &List, idx1: Vec<usize>) -> List {
        let lhs_len = df1.index(1).unwrap().len();
        let lhs_complement: Vec<usize> = (1..=lhs_len).filter(|i| !idx1.contains(i)).collect();
        let (names, combined) = subset_and_label(df1, &lhs_complement);
        List::from_names_and_values(names, combined).unwrap()
    }
}
