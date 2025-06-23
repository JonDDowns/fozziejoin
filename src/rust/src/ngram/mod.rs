use crate::utils::robj_index_map;
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
pub mod cosine;
pub mod jaccard;
pub mod qgram;

// Define a trait for string distance calculations
pub trait QGramDistance: Send + Sync {
    fn compute(&self, s1: &HashMap<&str, usize>, s2: &HashMap<&str, usize>) -> f64;

    #[cfg(not(target_os = "windows"))]
    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        q: usize,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
        if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
        };

        let map1 = robj_index_map(&left, &left_key);

        // This map uses qgrams as keys and keeps track of both frequencies
        // and the number of occurrences of each qgram
        let map2_qgrams = strvec_to_qgram_map(right, right_key, q);

        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .par_iter()
            .filter_map(|(k1, v1)| {
                let out =
                    self.compare_string_to_qgram_map(full, k1, v1, &map2_qgrams, q, max_distance);
                out
            })
            .flatten()
            .collect();
        idxs
    }

    #[cfg(target_os = "windows")]
    fn fuzzy_indices(
        &self,
        left: &List,
        left_key: &str,
        right: &List,
        right_key: &str,
        max_distance: f64,
        q: usize,
        full: bool,
        nthread: Option<usize>,
    ) -> Vec<(usize, usize, Option<f64>)> {
        let nt = if let Some(nt) = nthread {
            ThreadPoolBuilder::new()
                .num_threads(nt)
                .build()
                .expect("Global pool already initialized");
            nt
        } else {
            rayon::current_num_threads()
        };

        let map1 = robj_index_map(&left, &left_key);

        let batch_size = map1.len().div_ceil(nt);

        // This map uses qgrams as keys and keeps track of both frequencies
        // and the number of occurrences of each qgram
        let map2_qgrams = strvec_to_qgram_map(right, right_key, q);

        let idxs: Vec<(usize, usize, Option<f64>)> = map1
            .iter()
            .collect::<Vec<(&&str, &Vec<usize>)>>()
            .par_chunks(batch_size)
            .flat_map_iter(|chunk| {
                chunk.iter().filter_map(|(k1, v1)| {
                    self.compare_string_to_qgram_map(full, k1, v1, &map2_qgrams, q, max_distance)
                })
            })
            .flatten()
            .collect();
        idxs
    }

    fn compare_string_to_qgram_map(
        &self,
        full: bool,
        k1: &str,
        v1: &Vec<usize>,
        map2_qgrams: &HashMap<&str, (HashMap<&str, usize>, Vec<usize>)>,
        q: usize,
        max_distance: f64,
    ) -> Option<Vec<(usize, usize, Option<f64>)>> {
        if !full {
            if k1.is_na() {
                return None;
            }
        }

        let mut idxs: Vec<(usize, usize, Option<f64>)> = Vec::new();
        let qg1 = get_qgrams(k1, q);

        for (k2, (qg2, v2)) in map2_qgrams.iter() {
            if k2.len() < q && !full {
                continue;
            }

            if &k1 == k2 {
                iproduct!(v1, v2).for_each(|(v1, v2)| {
                    idxs.push((*v1, *v2, Some(0.)));
                });
                continue;
            }

            let dist = self.compute(&qg1, &qg2) as f64;
            if dist as f64 <= max_distance || full {
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
    }
}

fn get_qgrams(s: &str, q: usize) -> HashMap<&str, usize> {
    let mut qgram_map = HashMap::new();

    if s.len() < q {
        return qgram_map;
    }

    let mut char_indices = s.char_indices().collect::<Vec<_>>();
    char_indices.push((s.len(), '\0')); // Sentinel to get the final slice

    for i in 0..=char_indices.len().saturating_sub(q + 1) {
        let start = char_indices[i].0;
        let end = char_indices[i + q].0;
        let qgram = &s[start..end];
        *qgram_map.entry(qgram).or_insert(0) += 1;
    }

    qgram_map
}

pub fn strvec_to_qgram_map<'a>(
    df: &'a List,
    key: &'a str,
    q: usize,
) -> HashMap<&'a str, (HashMap<&'a str, usize>, Vec<usize>)> {
    let mut qgram_map: HashMap<&'a str, (HashMap<&'a str, usize>, Vec<usize>)> = HashMap::new();

    df.dollar(key)
        .expect(&format!("Column {key} does not exist or is not string."))
        .as_str_iter()
        .expect(&format!("Column {key} does not exist or is not string."))
        .enumerate()
        .for_each(|(index, val)| {
            let hm: HashMap<&str, usize> = get_qgrams(val, q);
            qgram_map
                .entry(val)
                .and_modify(|v| v.1.push(index + 1))
                .or_insert((hm, vec![index + 1]));
        });

    qgram_map
}
