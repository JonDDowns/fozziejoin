// All text distance algorithms either directly use or import the
// the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::ngram::QGramDistance;

// Cosine Distance Implementation
pub struct Jaccard;

use std::collections::{HashMap, HashSet};

impl QGramDistance for Jaccard {
    fn compute(&self, qgrams_s1: &HashMap<&str, usize>, qgrams_s2: &HashMap<&str, usize>) -> f64 {
        let mut intersection = 0;
        let mut union = 0;

        let mut all_keys: HashSet<_> = qgrams_s1.keys().cloned().collect();
        all_keys.extend(qgrams_s2.keys().cloned());

        for key in all_keys {
            let count1 = qgrams_s1.get(&key).copied().unwrap_or(0);
            let count2 = qgrams_s2.get(&key).copied().unwrap_or(0);

            intersection += count1.min(count2);
            union += count1.max(count2);
        }

        if union == 0 {
            1.0
        } else {
            1.0 - (intersection as f64 / union as f64)
        }
    }
}
