use crate::utils::robj_index_map;
use anyhow::Result;
use extendr_api::prelude::*;
use itertools::iproduct;
use rayon::prelude::*;
use rayon::ThreadPool;
use rustc_hash::FxHashMap;

pub struct Soundex;
impl Soundex {
    pub fn fuzzy_indices(
        &self,
        df1: &List,
        left_key: &str,
        df2: &List,
        right_key: &str,
        pool: &ThreadPool,
    ) -> Result<Vec<(usize, usize, f64)>> {
        let map1 = robj_index_map(&df1, left_key)?;
        let map2 = robj_index_map(&df2, right_key)?;

        let idxs: Vec<(usize, usize, f64)> = pool.install(|| {
            map1.par_iter()
                .filter_map(|(k1, v1)| self.compare_one_to_many(k1, v1, &map2))
                .flatten()
                .collect()
        });
        Ok(idxs)
    }

    pub fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        pool: &ThreadPool,
    ) -> Result<(Vec<usize>, Vec<f64>)> {
        let out = pool.install(|| {
            left.par_iter()
                .zip(right)
                .enumerate()
                .filter_map(|(i, (l, r))| {
                    if l.is_na() || r.is_na() {
                        return None;
                    }

                    let (sx_l, alt_l) = soundex_na_dual(l);
                    let (sx_r, alt_r) = soundex_na_dual(r);

                    if sx_l == sx_r
                        || alt_l == Some(sx_r)
                        || alt_r == Some(sx_l)
                        || (alt_l.is_some() && alt_r.is_some() && alt_l == alt_r)
                    {
                        Some((i, 0.))
                    } else {
                        None
                    }
                })
                .collect()
        });
        Ok(out)
    }

    fn compare_one_to_many(
        &self,
        k1: &str,
        v1: &Vec<usize>,
        idx_map: &FxHashMap<&str, Vec<usize>>,
    ) -> Option<Vec<(usize, usize, f64)>> {
        if k1.is_na() {
            return None;
        }

        let mut idxs: Vec<(usize, usize, f64)> = Vec::new();
        let (sx1, alt1) = soundex_na_dual(k1);
        let sx1 = sx1.as_str();

        for (k2, v2) in idx_map.iter() {
            if k2.is_na() {
                continue;
            }

            let (sx2, alt2) = soundex_na_dual(k2);

            if sx1 == sx2
                || alt1 == Some(sx2)
                || alt2 == Some(sx1.to_string())
                || (alt1.is_some() && alt2.is_some() && alt1 == alt2)
            {
                iproduct!(v1, v2).for_each(|(a, b)| {
                    idxs.push((*a, *b, 0.));
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

pub fn soundex_na(s: &str) -> String {
    let mut chars = s
        .chars()
        .filter(|c| c.is_ascii_alphabetic())
        .map(|c| c.to_ascii_uppercase());

    let first_letter = match chars.next() {
        Some(c) => c,
        None => return String::from("0000"),
    };

    let mut result = String::from(first_letter);
    let mut last_digit = encode_na(first_letter);
    let mut last_was_ignored = false;

    for c in chars {
        let digit = encode_na(c);
        if digit == '0' {
            last_was_ignored = true;
            continue;
        }
        if digit != last_digit || last_was_ignored {
            result.push(digit);
            last_digit = digit;
        }
        last_was_ignored = false;
    }

    result.truncate(4);
    while result.len() < 4 {
        result.push('0');
    }

    result
}

fn encode_na(c: char) -> char {
    match c {
        'B' | 'F' | 'P' | 'V' => '1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
        'D' | 'T' => '3',
        'L' => '4',
        'M' | 'N' => '5',
        'R' => '6',
        // A, E, I, O, U, H, W, Y are ignored
        _ => '0',
    }
}

pub fn soundex_na_dual(name: &str) -> (String, Option<String>) {
    let prefixes = [
        "DE", "LA", "LE", "VAN", "VON", "DI", "O", "CON", "BIN", "ABU", "AL", "SAN", "SANTA",
    ];

    // Clean and normalize
    let cleaned = name
        .chars()
        .filter(|c| c.is_ascii_alphabetic() || c.is_whitespace())
        .collect::<String>();

    // Split by whitespace and camel-case
    let tokens: Vec<String> = cleaned
        .split_whitespace()
        .flat_map(|t| split_double_capitals(t))
        .filter(|t| t.chars().all(|c| c.is_ascii_alphabetic()))
        .map(|t| t.to_ascii_uppercase())
        .collect();

    let mut prefix_parts = Vec::new();
    let mut root = None;

    for token in &tokens {
        if root.is_none() && prefixes.contains(&token.as_str()) {
            prefix_parts.push(token.clone());
        } else if root.is_none() {
            root = Some(token.clone());
        }
    }

    let primary = soundex_na(&root.unwrap_or_else(|| name.to_ascii_uppercase()));

    let alt = if !prefix_parts.is_empty() {
        let mut prefix_string = prefix_parts.join(" ");
        let mut code = soundex_na(&prefix_string);

        if code.len() < 4 {
            for token in &tokens[prefix_parts.len()..] {
                prefix_string.push(' ');
                prefix_string.push_str(token);
                code = soundex_na(&prefix_string);
                if code.len() == 4 {
                    break;
                }
            }
        }

        Some(code)
    } else {
        None
    };

    (primary, alt)
}

// Helper: splits camel-case like "VanDeusen" into ["Van", "Deusen"]
fn split_double_capitals(s: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut start = 0;
    let chars: Vec<char> = s.chars().collect();

    for i in 1..chars.len() {
        if chars[i - 1].is_uppercase() && chars[i].is_uppercase() {
            continue; // skip consecutive capitals
        }
        if chars[i].is_uppercase() && chars[i - 1].is_lowercase() {
            tokens.push(chars[start..i].iter().collect());
            start = i;
        }
    }

    tokens.push(chars[start..].iter().collect());
    tokens
}
