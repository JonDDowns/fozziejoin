pub mod edit;
pub mod ngram;
pub mod normalized;
pub mod stringdist;

use crate::string::edit::{
    damerau_levenshtein::DamerauLevenshtein, hamming::Hamming, lcs::LCSStr,
    levenshtein::Levenshtein, osa::OSA, EditDistance,
};
use crate::string::ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use crate::string::normalized::{jaro_winkler::JaroWinkler, NormalizedEditDistance};
use crate::utils::{get_pool, robj_index_map};
use crate::Merge;

use anyhow::{anyhow, Result};
use extendr_api::prelude::*;

pub fn string_join(
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
    let keys: Vec<(String, String)> = by
        .iter()
        .map(|(left_key, val)| {
            let right_key = val.as_string_vector().expect("lul");
            (left_key.to_string(), right_key[0].clone())
        })
        .collect();

    let pool = get_pool(nthread);
    let (left_key, right_key) = &keys[0];

    let mut matchdat: Vec<(usize, usize, f64)> = match method.as_str() {
        "osa" => OSA.fuzzy_indices(&df1, left_key, &df2, right_key, max_distance, &pool),
        "levenshtein" | "lv" => {
            Levenshtein.fuzzy_indices(&df1, left_key, &df2, right_key, max_distance, &pool)
        }
        "damerau_levensthein" | "dl" => {
            DamerauLevenshtein.fuzzy_indices(&df1, left_key, &df2, right_key, max_distance, &pool)
        }
        "hamming" => Hamming.fuzzy_indices(&df1, left_key, &df2, right_key, max_distance, &pool),
        "lcs" => LCSStr.fuzzy_indices(&df1, left_key, &df2, right_key, max_distance, &pool),
        "qgram" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            QGram.fuzzy_indices(
                &df1,
                left_key,
                &df2,
                right_key,
                max_distance,
                qz as usize,
                &pool,
            )
        }
        "cosine" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            Cosine.fuzzy_indices(
                &df1,
                left_key,
                &df2,
                right_key,
                max_distance,
                qz as usize,
                &pool,
            )
        }
        "jaccard" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            Jaccard.fuzzy_indices(
                &df1,
                left_key,
                &df2,
                right_key,
                max_distance,
                qz as usize,
                &pool,
            )
        }
        "jaro_winkler" | "jw" => {
            let map1 = robj_index_map(&df1, left_key);
            let map2 = robj_index_map(&df2, right_key);

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
    let out: List = if keys.len() == 1 {
        let joined = match how.as_str() {
            "inner" => Merge::inner_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
            "left" => Merge::left_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
            "right" => Merge::right_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
            "anti" => Merge::anti(&df1, idxs1),
            "full" => Merge::full_single(&df1, &df2, idxs1, idxs2, distance_col, &dists),
            _ => panic!("Problem with join logic!"),
        };
        joined
    } else {
        let mut dists = vec![dists];

        for bypair in keys[1..].iter() {
            (idxs1, idxs2, dists) = difference_pairs(
                &df1,
                &idxs1,
                &df2,
                &idxs2,
                &bypair,
                &dists,
                max_distance,
                &method,
                q,
                max_prefix,
                prefix_weight,
            )
            .expect("ruhoh");
        }
        let joined = match how.as_str() {
            "inner" => Merge::inner(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
            "left" => Merge::left(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
            "right" => Merge::right(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
            "anti" => Merge::anti(&df1, idxs1),
            "full" => Merge::full(&df1, &df2, idxs1, idxs2, distance_col, &dists, by),
            _ => panic!("I got 99 problems and a join is one"),
        };
        joined
    };
    Ok(data_frame!(out))
}

pub fn difference_pairs(
    df1: &List,
    idxs1: &Vec<usize>,
    df2: &List,
    idxs2: &Vec<usize>,
    by: &(String, String),
    dists: &Vec<Vec<f64>>,
    max_distance: f64,
    method: &str,
    q: Option<i32>,
    max_prefix: Option<i32>,
    prefix_weight: Option<f64>,
) -> Result<(Vec<usize>, Vec<usize>, Vec<Vec<f64>>)> {
    let lk = by.0.as_str();
    let rk = by.1.as_str();

    let vec1_binding = df1.dollar(lk).expect("lul").slice(idxs1).expect("ruhroh");
    let vec1: Vec<&str> = vec1_binding.as_str_vector().expect("ohmy");

    let vec2_binding = df2.dollar(rk).expect("lul").slice(idxs2).expect("ruhroh");
    let vec2: Vec<&str> = vec2_binding.as_str_vector().expect("ohmy");

    let (idxs0, newdist): (Vec<usize>, Vec<f64>) = match method {
        "osa" => OSA.compare_pairs(&vec1, &vec2, &max_distance),
        "levenshtein" | "lv" => Levenshtein.compare_pairs(&vec1, &vec2, &max_distance),
        "damerau_levensthein" | "dl" => {
            DamerauLevenshtein.compare_pairs(&vec1, &vec2, &max_distance)
        }
        "hamming" => Hamming.compare_pairs(&vec1, &vec2, &max_distance),
        "lcs" => LCSStr.compare_pairs(&vec1, &vec2, &max_distance),
        "qgram" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            let qz = qz as usize;
            QGram.compare_pairs(&vec1, &vec2, &qz, &max_distance)
        }
        "cosine" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            let qz = qz as usize;
            Cosine.compare_pairs(&vec1, &vec2, &qz, &max_distance)
        }
        "jaccard" => {
            let qz = q.ok_or_else(|| anyhow!("Must provide `q` for method `{}`", method))?;
            let qz = qz as usize;
            Jaccard.compare_pairs(&vec1, &vec2, &qz, &max_distance)
        }
        "jaro_winkler" | "jw" => {
            let max_prefix =
                max_prefix.ok_or_else(|| anyhow!("Parameter `max_prefix` not provided"))? as usize;
            let prefix_weight =
                prefix_weight.ok_or_else(|| anyhow!("Parameter `prefix_weight` not provided"))?;

            let jw = JaroWinkler {};
            jw.compare_pairs(&vec1, &vec2, &max_distance, prefix_weight, max_prefix)
        }
        _ => return Err(anyhow!("The join method `{}` is not supported", method)),
    };

    let (idxs1b, idxs2b) = { idxs0.iter().map(|&i| (idxs1[i], idxs2[i])).unzip() };

    let mut dists_out = vec![];
    for distvec in dists {
        let tmp: Vec<f64> = idxs0.iter().map(|&i| distvec[i]).collect();
        dists_out.push(tmp);
    }
    dists_out.push(newdist);
    Ok((idxs1b, idxs2b, dists_out))
}
