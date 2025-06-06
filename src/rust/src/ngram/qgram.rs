use crate::ngram::{get_qgrams, QGramDistance};

// Q-Gram Distance Implementation
pub struct QGram;

impl QGramDistance for QGram {
    fn compute(&self, s1: &str, s2: &str, q: usize) -> f64 {
        let qgrams_s1 = get_qgrams(s1, q);
        let qgrams_s2 = get_qgrams(s2, q);

        let mut mismatch_count = 0;

        for (qgram, &count1) in &qgrams_s1 {
            let count2 = qgrams_s2.get(qgram).unwrap_or(&0);
            mismatch_count += (count1 as i32 - *count2 as i32).abs();
        }

        for (qgram, &count2) in &qgrams_s2 {
            if !qgrams_s1.contains_key(qgram) {
                mismatch_count += count2 as i32;
            }
        }

        mismatch_count as f64
    }
}
