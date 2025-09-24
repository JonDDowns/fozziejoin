// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::string::ngram::QGramDistance;
use rustc_hash::FxHashMap;

// Q-Gram Distance Implementation
pub struct QGram;

impl QGramDistance for QGram {
    fn compute(
        &self,
        qgrams_s1: &FxHashMap<&str, usize>,
        qgrams_s2: &FxHashMap<&str, usize>,
    ) -> f64 {
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
