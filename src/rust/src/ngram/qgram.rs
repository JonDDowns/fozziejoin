// All text distance algorithms either directly use or import the
// the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::ngram::QGramDistance;
use std::collections::HashMap;

// Q-Gram Distance Implementation
pub struct QGram;

impl QGramDistance for QGram {
    fn compute(&self, qgrams_s1: &HashMap<&str, usize>, qgrams_s2: &HashMap<&str, usize>) -> f64 {
        let mut mismatch_count = 0;

        for (qgram, &count1) in qgrams_s1 {
            let count2 = qgrams_s2.get(qgram).unwrap_or(&0);
            mismatch_count += (count1 as i32 - *count2 as i32).abs();
        }

        for (qgram, &count2) in qgrams_s2 {
            if !qgrams_s1.contains_key(qgram) {
                mismatch_count += count2 as i32;
            }
        }

        mismatch_count as f64
    }
}
