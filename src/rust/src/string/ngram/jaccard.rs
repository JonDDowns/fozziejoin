// This text distance is adapted from the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use anyhow::{anyhow, Result};
use extendr_api::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPool;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;

use crate::string::ngram::QGramDistance;

// Cosine Distance Implementation
pub struct Jaccard;

fn get_qgram_set(s: &str, q: usize) -> FxHashSet<&str> {
    let mut grams = FxHashSet::default();
    let mut ring = VecDeque::with_capacity(q + 1);

    for (i, _) in s.char_indices() {
        ring.push_back(i);
        if ring.len() == q + 1 {
            let start = ring[0];
            let end = ring[q];
            grams.insert(&s[start..end]);
            ring.pop_front();
        }
    }

    if ring.len() == q {
        let start = ring[0];
        let end = s.len();
        grams.insert(&s[start..end]);
    }

    grams
}

impl QGramDistance for Jaccard {
    fn compute(
        &self,
        qgrams_s1: &FxHashMap<&str, usize>,
        qgrams_s2: &FxHashMap<&str, usize>,
    ) -> f64 {
        let mut intersection = 0;
        let mut union = 0;

        let mut all_keys: FxHashSet<_> = qgrams_s1.keys().cloned().collect();
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

    fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        q: &usize,
        max_distance: &f64,
        pool: &rayon::ThreadPool,
    ) -> (Vec<usize>, Vec<f64>) {
        let (keep, dists): (Vec<usize>, Vec<f64>) = pool.install(|| {
            left.par_iter()
                .zip(right)
                .enumerate()
                .filter_map(|(i, (l, r))| {
                    if l.is_na() || r.is_na() {
                        return None;
                    }

                    let hs1 = get_qgram_set(l, *q);
                    let hs2 = get_qgram_set(r, *q);

                    let dist = if hs1.is_empty() && hs2.is_empty() {
                        0.0
                    } else {
                        let intersection = hs1.intersection(&hs2).count();
                        let union = hs1.union(&hs2).count();
                        1.0 - (intersection as f64 / union as f64)
                    };

                    if dist <= *max_distance {
                        Some((i, dist))
                    } else {
                        None
                    }
                })
                .unzip()
        });

        (keep, dists)
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
    ) -> Result<Vec<(usize, usize, f64)>> {
        // Build RHS q-gram reverse index
        let mut rhs_qgram_index: FxHashMap<&str, Vec<usize>> = FxHashMap::default();
        let mut rhs_qgrams: FxHashMap<usize, FxHashSet<&str>> = FxHashMap::default();

        let right_iter = right
            .dollar(right_key)
            .map_err(|_| anyhow!("Column '{}' not found in right dataframe", right_key))?
            .as_str_iter()
            .ok_or_else(|| anyhow!("Column '{}' is not a string vector", right_key))?;

        for (r_idx, val) in right_iter.enumerate() {
            let idx = r_idx + 1;
            let grams = get_qgram_set(val, q);
            rhs_qgrams.insert(idx, grams.clone());
            for gram in grams {
                rhs_qgram_index.entry(gram).or_default().push(idx);
            }
        }

        // Match LHS records to RHS candidates via shared q-grams
        let left_proto = left
            .dollar(left_key)
            .map_err(|_| anyhow!("Column '{}' not found in left dataframe", left_key))?;
        let left_iter = left_proto
            .as_str_vector()
            .ok_or_else(|| anyhow!("Column '{}' is not a string vector", left_key))?;

        let results = pool.install(|| {
            left_iter
                .par_iter()
                .enumerate()
                .filter_map(|(l_idx, val)| {
                    let lhs_idx = l_idx + 1;
                    let lhs_grams = get_qgram_set(val, q);

                    // Collect RHS candidates that share at least one q-gram
                    let mut candidates = FxHashSet::default();
                    for gram in &lhs_grams {
                        if let Some(rhs_idxs) = rhs_qgram_index.get(gram) {
                            candidates.extend(rhs_idxs);
                        }
                    }

                    if candidates.is_empty() {
                        return None;
                    }

                    // Compare Jaccard distance for each candidate
                    let mut matches = Vec::new();
                    for &rhs_idx in &candidates {
                        let rhs_grams = &rhs_qgrams[&rhs_idx];

                        // Predict best-case similarity
                        let max_intersection = lhs_grams.len().min(rhs_grams.len());
                        let min_union = lhs_grams.len().max(rhs_grams.len());
                        let min_possible_distance =
                            1.0 - (max_intersection as f64 / min_union as f64);

                        if min_possible_distance > max_distance {
                            continue; // Skip: can't possibly be close enough
                        }

                        // Proceed with actual Jaccard distance
                        let intersection = lhs_grams.intersection(rhs_grams).count();
                        let union = lhs_grams.union(rhs_grams).count();
                        let dist = 1.0 - (intersection as f64 / union as f64);

                        if dist <= max_distance {
                            matches.push((lhs_idx, rhs_idx, dist));
                        }
                    }
                    Some(matches)
                })
                .flatten()
                .collect()
        });

        Ok(results)
    }
}
