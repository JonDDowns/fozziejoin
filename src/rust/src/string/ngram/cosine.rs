// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::string::ngram::QGramDistance;
use rustc_hash::FxHashMap;

// Cosine Distance Implementation
pub struct Cosine;

impl QGramDistance for Cosine {
    fn compute(
        &self,
        qgrams_s1: &FxHashMap<&str, usize>,
        qgrams_s2: &FxHashMap<&str, usize>,
    ) -> f64 {
        let mut dot_product = 0;
        let mut norm_s1 = 0;
        let mut norm_s2 = 0;

        // Compute dot product and vector norms
        for (qgram, &count1) in qgrams_s1 {
            if let Some(&count2) = qgrams_s2.get(qgram) {
                dot_product += count1 * count2;
            }
            norm_s1 += count1 * count1;
        }

        for &count2 in qgrams_s2.values() {
            norm_s2 += count2 * count2;
        }

        if norm_s1 == 0 || norm_s2 == 0 {
            return 1.0; // Maximum distance if no similarity
        }

        let similarity = dot_product as f64 / (norm_s1 as f64).sqrt() / (norm_s2 as f64).sqrt();
        1.0 - similarity // Convert similarity to edit distance
    }
}
