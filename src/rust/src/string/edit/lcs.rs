use crate::string::edit::EditDistance;
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

pub struct LCSStr;

impl LCSStr {
    fn compute(&self, s1: &str, s2: &str) -> usize {
        let m = s1.len();
        let n = s2.len();
        let mut dp = vec![vec![0; n + 1]; m + 1];

        for (i, c1) in s1.chars().enumerate() {
            for (j, c2) in s2.chars().enumerate() {
                if c1 == c2 {
                    dp[i + 1][j + 1] = dp[i][j] + 1;
                } else {
                    dp[i + 1][j + 1] = dp[i + 1][j].max(dp[i][j + 1]);
                }
            }
        }

        (m + n) - 2 * dp[m][n]
    }
}

impl EditDistance for LCSStr {
    fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
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
                    let dist = self.compute(l, r) as f64;
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

    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        length_map: &FxHashMap<usize, Vec<&str>>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
        max_distance: &f64,
    ) -> Option<Vec<(usize, usize, f64)>> {
        // Skip all comparisons if string is NA
        if k1.is_na() {
            return None;
        }

        // Get range of lengths within max distance of current
        let k1_len = k1.len();
        let start_len = k1_len.saturating_sub(*max_distance as usize);
        let end_len = k1_len.saturating_add(*max_distance as usize + 1);

        // Start a list to collect results
        let mut idxs: Vec<(usize, usize, f64)> = Vec::new();

        // Begin making string comparisons
        for i in start_len..end_len {
            if let Some(lookup) = length_map.get(&i) {
                lookup.iter().for_each(|k2| {
                    // Skip this iter if RHS is NA
                    if k2.is_na() {
                        return;
                    }

                    // No need to run distance functions if exactly the same
                    if &k1 == k2 {
                        let v2 = idx_map.get(k2).unwrap();
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, 0.));
                        });
                        return;
                    }

                    // Run distance calculation
                    let dist = self.compute(&k1, &k2) as f64;

                    // Check vs. threshold
                    if dist <= *max_distance {
                        let v2 = idx_map.get(k2).unwrap();
                        iproduct!(v1, v2).for_each(|(v1, v2)| {
                            idxs.push((*v1, *v2, dist));
                        });
                        return;
                    }
                });
            }
        }

        // Return all matches, if any
        if idxs.is_empty() {
            return None;
        } else {
            return Some(idxs);
        }
    }
}
