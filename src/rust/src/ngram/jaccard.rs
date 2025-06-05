use crate::ngram::QGramDistance;

// Cosine Distance Implementation
pub struct Jaccard;

use std::collections::HashSet;

/// Generate *n*-grams from a string
fn get_qgrams_set(s: &str, n: usize) -> HashSet<String> {
    s.chars()
        .collect::<Vec<_>>()
        .windows(n)
        .map(|window| window.iter().collect())
        .collect()
}

impl QGramDistance for Jaccard {
    /// Compute Jaccard distance for *n*-grams
    fn compute(&self, s1: &str, s2: &str, q: usize) -> f64 {
        let ngrams1 = get_qgrams_set(s1, q);
        let ngrams2 = get_qgrams_set(s2, q);

        let intersection_size = ngrams1.intersection(&ngrams2).count();
        let union_size = ngrams1.union(&ngrams2).count();

        if union_size == 0 {
            return 1.0; // Max distance when no shared elements
        }

        1.0 - (intersection_size as f64 / union_size as f64)
    }
}
