use crate::edit::EditDistance;
use textdistance::str::damerau_levenshtein;

pub struct DamerauLevenshtein;
impl EditDistance for DamerauLevenshtein {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        damerau_levenshtein(s1, s2)
    }
}

