// All text distance algorithms either directly use or import the
// the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::edit::EditDistance;
use textdistance::str::damerau_levenshtein;

pub struct DamerauLevenshtein;
impl EditDistance for DamerauLevenshtein {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        damerau_levenshtein(s1, s2)
    }
}
