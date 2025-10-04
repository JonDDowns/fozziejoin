use crate::merge::{subset_and_label, Merge};
use extendr_api::prelude::*;

impl Merge {
    pub fn semi(df1: &List, mut idx1: Vec<usize>) -> List {
        idx1.sort_unstable();
        idx1.dedup();

        let (names, combined) = subset_and_label(df1, &idx1);
        List::from_names_and_values(names, combined).unwrap()
    }
}
