pub mod edit;
pub mod jaro_winkler;
pub mod joinmethod;
pub mod ngram;

use crate::string::edit::{
    damerau_levenshtein::DamerauLevenshtein, hamming::Hamming, lcs::LCSStr,
    levenshtein::Levenshtein, osa::OSA, EditDistance,
};
use crate::string::jaro_winkler::JaroWinkler;
use crate::string::joinmethod::get_join_method;
use crate::string::ngram::{cosine::Cosine, jaccard::Jaccard, qgram::QGram, QGramDistance};
use crate::utils::get_pool;
use crate::Merge;

use anyhow::Result;
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

    let qz = match q {
        Some(x) => Some(x as usize),
        None => None,
    };

    let max_prefix = match max_prefix {
        Some(x) => Some(x as usize),
        None => None,
    };

    let prefix_weight = match prefix_weight {
        Some(x) => Some(x as f64),
        None => None,
    };

    let join_method = get_join_method(&method, max_distance, qz, prefix_weight, max_prefix)?;
    let mut matchdat = join_method.fuzzy_indices(&df1, left_key, &df2, right_key, &pool)?;
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
                qz,
                max_prefix,
                prefix_weight,
                &pool,
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
    q: Option<usize>,
    max_prefix: Option<usize>,
    prefix_weight: Option<f64>,
    pool: &rayon::ThreadPool,
) -> Result<(Vec<usize>, Vec<usize>, Vec<Vec<f64>>)> {
    let lk = by.0.as_str();
    let rk = by.1.as_str();

    let vec1_binding = df1.dollar(lk).expect("lul").slice(idxs1).expect("ruhroh");
    let vec1: Vec<&str> = vec1_binding.as_str_vector().expect("ohmy");

    let vec2_binding = df2.dollar(rk).expect("lul").slice(idxs2).expect("ruhroh");
    let vec2: Vec<&str> = vec2_binding.as_str_vector().expect("ohmy");

    let join_method = get_join_method(method, max_distance, q, prefix_weight, max_prefix)?;
    let (idxs0, newdist) = join_method.compare_pairs(&vec1, &vec2, pool)?;
    let (idxs1b, idxs2b) = { idxs0.iter().map(|&i| (idxs1[i], idxs2[i])).unzip() };

    let mut dists_out = vec![];
    for distvec in dists {
        let tmp: Vec<f64> = idxs0.iter().map(|&i| distvec[i]).collect();
        dists_out.push(tmp);
    }
    dists_out.push(newdist);
    Ok((idxs1b, idxs2b, dists_out))
}
