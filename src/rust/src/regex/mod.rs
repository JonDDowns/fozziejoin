use anyhow::{anyhow, Result};
use extendr_api::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPool;
use regex::{RegexBuilder, RegexSetBuilder};

pub fn fuzzy_indices_regex(
    values: &[&str],
    patterns: &[&str],
    ignore_case: bool,
    pool: &ThreadPool,
) -> Result<(Vec<usize>, Vec<usize>)> {
    // Compile RegexSet from all patterns
    let regex_set = RegexSetBuilder::new(patterns)
        .case_insensitive(ignore_case)
        .build()
        .map_err(|e| anyhow!("Failed to build RegexSet: {}", e))?;

    // Parallel match over values
    let (lhs_indices, rhs_indices): (Vec<usize>, Vec<usize>) = pool.install(|| {
        values
            .par_iter()
            .enumerate()
            .flat_map_iter(|(i_idx, value)| {
                regex_set
                    .matches(value)
                    .into_iter()
                    .map(move |j_idx| (i_idx + 1, j_idx + 1))
            })
            .unzip()
    });

    Ok((lhs_indices, rhs_indices))
}

pub fn regex_join(
    df1: &List,
    df2: &List,
    by: (String, String),
    ignore_case: bool,
    pool: &ThreadPool,
) -> Result<(Vec<usize>, Vec<usize>)> {
    let lk = by.0.as_str();
    let rk = by.1.as_str();

    let values_binding = df1
        .dollar(lk)
        .map_err(|_| anyhow!("Column `{}` not found in df1", lk))?;

    let values_vec = values_binding
        .as_str_vector()
        .ok_or_else(|| anyhow!("Column `{}` in df1 is not a string", lk))?;

    let patterns_binding = df2
        .dollar(rk)
        .map_err(|_| anyhow!("Column `{}` not found in df2", rk))?;

    let patterns_vec = patterns_binding
        .as_str_vector()
        .ok_or_else(|| anyhow!("Column `{}` in df2 is not a string", rk))?;

    fuzzy_indices_regex(&values_vec, &patterns_vec, ignore_case, pool)
}

pub fn regex_pairs(
    df1: &List,
    idxs1: &Vec<usize>,
    df2: &List,
    idxs2: &Vec<usize>,
    by: &(String, String),
    ignore_case: bool,
    pool: &ThreadPool,
) -> Result<(Vec<usize>, Vec<usize>)> {
    let lk = by.0.as_str();
    let rk = by.1.as_str();

    let values_binding = df1
        .dollar(lk)
        .map_err(|_| anyhow!("Column `{}` not found in df1", lk))?;

    let vec1 = values_binding
        .as_str_vector()
        .ok_or_else(|| anyhow!("Column `{}` in df1 is not a string", lk))?;

    let patterns_binding = df2
        .dollar(rk)
        .map_err(|_| anyhow!("Column `{}` not found in df2", rk))?;

    let vec2 = patterns_binding
        .as_str_vector()
        .ok_or_else(|| anyhow!("Column `{}` in df2 is not a string", rk))?;

    let matched: Vec<(usize, usize)> = pool.install(|| {
        vec1.par_iter()
            .zip(vec2)
            .enumerate()
            .filter_map(|(i, (pattern, target))| {
                RegexBuilder::new(pattern)
                    .case_insensitive(ignore_case)
                    .build()
                    .ok()
                    .and_then(|re| {
                        if re.is_match(&target) {
                            Some((idxs1[i], idxs2[i]))
                        } else {
                            None
                        }
                    })
            })
            .collect()
    });

    let (idxs1b, idxs2b): (Vec<usize>, Vec<usize>) = matched.into_iter().unzip();
    Ok((idxs1b, idxs2b))
}
