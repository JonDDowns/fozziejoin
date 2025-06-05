use crate::ngram::{get_qgrams, QGramDistance};

// Q-Gram Distance Implementation
pub struct QGram;

impl QGramDistance for QGram {
    fn compute(&self, s1: &str, s2: &str, q: usize) -> f64 {
        let qgrams_s1 = get_qgrams(s1, q);
        let qgrams_s2 = get_qgrams(s2, q);

        let mut dot_product = 0;
        let mut norm_s1 = 0;
        let mut norm_s2 = 0;

        for (qgram, &count1) in &qgrams_s1 {
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
