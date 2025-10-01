pub mod edit;
pub mod ngram;
pub mod normalized;

use crate::string::edit::{
    damerau_levenshtein::DamerauLevenshtein, hamming::Hamming, lcs::LCSStr,
    levenshtein::Levenshtein, osa::OSA, EditDistance,
};
use crate::string::ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use crate::string::normalized::{jaro_winkler::JaroWinkler, NormalizedEditDistance};
use crate::utils::{get_pool, robj_index_map, transpose_map_fx};
use crate::Merge;

use anyhow::{anyhow, Result};
use extendr_api::prelude::*;
use rustc_hash::FxHashMap;

pub fn string_multi_join(
    df1: List,
    df2: List,
    by: List,
    method: String,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    q: Option<i32>,
    max_prefix: Option<i32>,
    prefix_weight: Option<f64>,
    nthread: Option<usize>,
) -> Result<Robj> {
    let mut keep_idxs: FxHashMap<(usize, usize), Vec<f64>> = FxHashMap::default();
    let pool = get_pool(nthread);

    for (match_iter, (left_key, right_key)) in by.iter().enumerate() {
        let rk_vec = right_key
            .as_str_vector()
            .ok_or_else(|| anyhow!("Right key {:?} is not a string vector", right_key))?;

        let rk = rk_vec
            .get(0)
            .ok_or_else(|| anyhow!("Right key vector is empty"))?;

        let matchdat = match method.as_str() {
            "osa" => OSA.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
            "levenshtein" | "lv" => {
                Levenshtein.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool)
            }
            "damerau_levensthein" | "dl" => {
                DamerauLevenshtein.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool)
            }
            "hamming" => Hamming.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
            "lcs" => LCSStr.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
            "qgram" => {
                let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
                QGram.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
            }
            "cosine" => {
                let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
                Cosine.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
            }
            "jaccard" => {
                let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
                Jaccard.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
            }
            "jaro_winkler" | "jw" => {
                let map1 = robj_index_map(&df1, &left_key);
                let map2 = robj_index_map(&df2, rk);

                let max_prefix = max_prefix
                    .ok_or_else(|| anyhow!("Parameter `max_prefix` not provided"))?
                    as usize;
                let prefix_weight = prefix_weight
                    .ok_or_else(|| anyhow!("Parameter `prefix_weight` not provided"))?;

                let jw = JaroWinkler {};
                jw.fuzzy_indices(map1, map2, max_distance, prefix_weight, max_prefix, &pool)
            }
            _ => return Err(anyhow!("The join method `{}` is not supported", method)),
        };

        if match_iter == 0 {
            keep_idxs = matchdat
                .iter()
                .map(|(a, b, c)| ((*a, *b), vec![*c]))
                .collect();
        } else {
            let idxs: Vec<(usize, usize)> = matchdat.iter().map(|(a, b, _)| (*a, *b)).collect();
            keep_idxs.retain(|key, _| idxs.contains(key));

            for (id1, id2, dist) in matchdat {
                if let Some(existing) = keep_idxs.get_mut(&(id1, id2)) {
                    existing.push(dist);
                }
            }
        }
    }

    let (idxs1, idxs2, dists) = transpose_map_fx(keep_idxs);

    let out = match how.as_str() {
        "inner" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        "anti" => Merge::anti(&df1, idxs1),
        "full" => Merge::full(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
        _ => return Err(anyhow!("Join type `{}` not supported", how)),
    };

    Ok(data_frame!(out))
}

pub fn string_single_join(
    df1: List,
    df2: List,
    by: List,
    method: String,
    how: String,
    max_distance: f64,
    distance_col: Option<String>,
    q: Option<i32>,
    max_prefix: Option<i32>,
    prefix_weight: Option<f64>,
    nthread: Option<usize>,
) -> Result<Robj> {
    let pool = get_pool(nthread);

    if by.len() != 1 {
        return Err(anyhow!(
            "Expected exactly one pair of match keys in `by`, found {}",
            by.len()
        ));
    }

    let (left_key, right_key) = by
        .iter()
        .next()
        .ok_or_else(|| anyhow!("No `by` arguments provided."))?;

    let rk_vec = right_key
        .as_str_vector()
        .ok_or_else(|| anyhow!("Right key {:?} is not a string vector", right_key))?;

    let rk = rk_vec
        .get(0)
        .ok_or_else(|| anyhow!("Right key vector is empty"))?;

    let mut matchdat = match method.as_str() {
        "osa" => OSA.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
        "levenshtein" | "lv" => {
            Levenshtein.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool)
        }
        "damerau_levensthein" | "dl" => {
            DamerauLevenshtein.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool)
        }
        "hamming" => Hamming.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
        "lcs" => LCSStr.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, &pool),
        "qgram" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            QGram.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
        }
        "cosine" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            Cosine.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
        }
        "jaccard" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            Jaccard.fuzzy_indices(&df1, left_key, &df2, rk, max_distance, qz as usize, &pool)
        }
        "jaro_winkler" | "jw" => {
            let map1 = robj_index_map(&df1, &left_key);
            let map2 = robj_index_map(&df2, rk);

            let max_prefix =
                max_prefix.ok_or_else(|| anyhow!("Parameter `max_prefix` not provided"))? as usize;
            let prefix_weight =
                prefix_weight.ok_or_else(|| anyhow!("Parameter `prefix_weight` not provided"))?;

            let jw = JaroWinkler {};
            jw.fuzzy_indices(map1, map2, max_distance, prefix_weight, max_prefix, &pool)
        }
        _ => return Err(anyhow!("The join method `{}` is not supported", method)),
    };
    matchdat.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));

    let mut idxs1 = Vec::with_capacity(matchdat.len());
    let mut idxs2 = Vec::with_capacity(matchdat.len());
    let mut dists = Vec::with_capacity(matchdat.len());

    for (i, j, d) in matchdat {
        idxs1.push(i);
        idxs2.push(j);
        dists.push(d);
    }

    let out = match how.as_str() {
        "inner" => Merge::inner_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "left" => Merge::left_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "right" => Merge::right_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        "anti" => Merge::anti(&df1, idxs1),
        "full" => Merge::full_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
        _ => return Err(anyhow!("Join type `{}` not supported", how)),
    };

    Ok(data_frame!(out))
}
