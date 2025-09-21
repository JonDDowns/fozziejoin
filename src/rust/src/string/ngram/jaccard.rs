// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use rayon::ThreadPool;

use crate::string::ngram::QGramDistance;

// Cosine Distance Implementation
pub struct Jaccard;

use std::collections::{HashMap, HashSet, VecDeque};

impl Jaccard {
    pub fn jaccard_distance<'a>(&self, a: &HashSet<&'a str>, b: &HashSet<&'a str>) -> f64 {
        if a.is_empty() && b.is_empty() {
            return 0.0; // no dissimilarity when both sets are empty
        }

        let intersection_size = a.intersection(b).count();
        let union_size = a.union(b).count();

        1.0 - (intersection_size as f64) / (union_size as f64)
    }
}

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

    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        q: usize,
        pool: &ThreadPool,
    ) -> Vec<(usize, usize, Option<f64>)> {
        let mut left_meta: HashMap<&str, (Vec<usize>, HashSet<&str>)> = HashMap::new();
        left.dollar(&left_key)
            .expect(&format!(
                "Column {right_key} does not exist or is not string."
            ))
            .as_str_iter()
            .expect(&format!(
                "Column {right_key} does not exist or is not string."
            ))
            .enumerate()
            .for_each(|(index, val)| {
                let entry = left_meta
                    .entry(val)
                    .or_insert_with(|| (Vec::new(), HashSet::new()));
                entry.0.push(index + 1);

                let mut ring = VecDeque::with_capacity(q + 1);

                for (i, _) in val.char_indices() {
                    ring.push_back(i);
                    if ring.len() == q + 1 {
                        let start = ring[0];
                        let end = ring[q];
                        entry.1.insert(&val[start..end]);
                        ring.pop_front(); // slide the window
                    }
                }

                // Handle final gram if at end of string
                if ring.len() == q {
                    let start = ring[0];
                    let end = val.len();
                    entry.1.insert(&val[start..end]);
                }
            });

        // This map uses qgrams as keys and keeps track of both frequencies
        // and the number of occurrences of each qgram
        let mut right_meta: HashMap<&str, (Vec<usize>, HashSet<&str>)> = HashMap::new();
        right
            .dollar(&right_key)
            .expect(&format!(
                "Column {right_key} does not exist or is not string."
            ))
            .as_str_iter()
            .expect(&format!(
                "Column {right_key} does not exist or is not string."
            ))
            .enumerate()
            .for_each(|(index, val)| {
                let entry = right_meta
                    .entry(val)
                    .or_insert_with(|| (Vec::new(), HashSet::new()));
                entry.0.push(index + 1);

                let mut ring = VecDeque::with_capacity(q + 1);

                for (i, _) in val.char_indices() {
                    ring.push_back(i);
                    if ring.len() == q + 1 {
                        let start = ring[0];
                        let end = ring[q];
                        entry.1.insert(&val[start..end]);
                        ring.pop_front(); // slide the window
                    }
                }

                // Handle final gram if at end of string
                if ring.len() == q {
                    let start = ring[0];
                    let end = val.len();
                    entry.1.insert(&val[start..end]);
                }
            });

        let idxs: Vec<(usize, usize, Option<f64>)> = pool.install(|| {
            left_meta
                .par_iter()
                .filter_map(|(_, (v1, hs1))| {
                    let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();

                    for (_, (v2, hs2)) in right_meta.iter() {
                        let dist = if hs1.is_empty() && hs2.is_empty() {
                            0.0
                        } else {
                            let intersection_size = hs1.intersection(hs2).count();
                            let union_size = hs1.union(hs2).count();
                            1.0 - (intersection_size as f64) / (union_size as f64)
                        };

                        if dist <= max_distance {
                            iproduct!(v1, v2).for_each(|(a, b)| {
                                idxs.push((*a, *b, Some(dist)));
                            });
                        }
                    }

                    if idxs.is_empty() {
                        None
                    } else {
                        Some(idxs)
                    }
                })
                .flatten()
                .collect()
        });
        idxs
    }
}
