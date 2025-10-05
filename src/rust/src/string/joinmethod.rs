pub enum JoinMethod {
    OSA {
        max_distance: f64,
    },
    Levenshtein {
        max_distance: f64,
    },
    DamerauLevenshtein {
        max_distance: f64,
    },
    Hamming {
        max_distance: f64,
    },
    LCS {
        max_distance: f64,
    },
    QGram {
        max_distance: f64,
        q: usize,
    },
    Cosine {
        max_distance: f64,
        q: usize,
    },
    Jaccard {
        max_distance: f64,
        q: usize,
    },
    JaroWinkler {
        max_distance: f64,
        prefix_weight: f64,
        max_prefix: usize,
    },
    Soundex {},
}

impl JoinMethod {
    pub fn fuzzy_indices(
        &self,
        left: &extendr_api::List,
        left_key: &str,
        right: &extendr_api::List,
        right_key: &str,
        pool: &rayon::ThreadPool,
    ) -> anyhow::Result<Vec<(usize, usize, f64)>> {
        use crate::string::*;

        let result =
            match self {
                JoinMethod::OSA { max_distance } => {
                    OSA.fuzzy_indices(left, left_key, right, right_key, *max_distance, pool)
                }
                JoinMethod::Levenshtein { max_distance } => {
                    Levenshtein.fuzzy_indices(left, left_key, right, right_key, *max_distance, pool)
                }
                JoinMethod::DamerauLevenshtein { max_distance } => DamerauLevenshtein
                    .fuzzy_indices(left, left_key, right, right_key, *max_distance, pool),
                JoinMethod::Hamming { max_distance } => {
                    Hamming.fuzzy_indices(left, left_key, right, right_key, *max_distance, pool)
                }
                JoinMethod::LCS { max_distance } => {
                    LCSStr.fuzzy_indices(left, left_key, right, right_key, *max_distance, pool)
                }
                JoinMethod::QGram { max_distance, q } => {
                    QGram.fuzzy_indices(left, left_key, right, right_key, *max_distance, *q, pool)
                }
                JoinMethod::Cosine { max_distance, q } => {
                    Cosine.fuzzy_indices(left, left_key, right, right_key, *max_distance, *q, pool)
                }
                JoinMethod::Jaccard { max_distance, q } => {
                    Jaccard.fuzzy_indices(left, left_key, right, right_key, *max_distance, *q, pool)
                }
                JoinMethod::JaroWinkler {
                    max_distance,
                    prefix_weight,
                    max_prefix,
                } => JaroWinkler.fuzzy_indices(
                    left,
                    left_key,
                    right,
                    right_key,
                    *max_distance,
                    *prefix_weight,
                    *max_prefix,
                    pool,
                ),
                JoinMethod::Soundex {} => {
                    Soundex.fuzzy_indices(left, left_key, right, right_key, pool)
                }
            }?;

        Ok(result)
    }

    pub fn compare_pairs(
        &self,
        left: &Vec<&str>,
        right: &Vec<&str>,
        pool: &rayon::ThreadPool,
    ) -> anyhow::Result<(Vec<usize>, Vec<f64>)> {
        use crate::string::*;

        let result = match self {
            JoinMethod::OSA { max_distance } => {
                Ok(OSA.compare_pairs(left, right, max_distance, pool))
            }
            JoinMethod::Levenshtein { max_distance } => {
                Ok(Levenshtein.compare_pairs(left, right, max_distance, pool))
            }
            JoinMethod::DamerauLevenshtein { max_distance } => {
                Ok(DamerauLevenshtein.compare_pairs(left, right, max_distance, pool))
            }
            JoinMethod::Hamming { max_distance } => {
                Ok(Hamming.compare_pairs(left, right, max_distance, pool))
            }
            JoinMethod::LCS { max_distance } => {
                Ok(LCSStr.compare_pairs(left, right, max_distance, pool))
            }
            JoinMethod::QGram { max_distance, q } => {
                Ok(QGram.compare_pairs(left, right, q, max_distance, pool))
            }
            JoinMethod::Cosine { max_distance, q } => {
                Ok(Cosine.compare_pairs(left, right, q, max_distance, pool))
            }
            JoinMethod::Jaccard { max_distance, q } => {
                Ok(Jaccard.compare_pairs(left, right, q, max_distance, pool))
            }
            JoinMethod::JaroWinkler {
                max_distance,
                prefix_weight,
                max_prefix,
            } => Ok(JaroWinkler.compare_pairs(
                left,
                right,
                max_distance,
                *prefix_weight,
                *max_prefix,
                pool,
            )),
            JoinMethod::Soundex {} => Soundex.compare_pairs(left, right, pool),
        };

        result
    }
}

pub fn get_join_method(
    method: &str,
    max_distance: f64,
    q: Option<usize>,
    prefix_weight: Option<f64>,
    max_prefix: Option<usize>,
) -> anyhow::Result<JoinMethod> {
    match method {
        "osa" => Ok(JoinMethod::OSA { max_distance }),
        "levenshtein" | "lv" => Ok(JoinMethod::Levenshtein { max_distance }),
        "damerau_levensthein" | "dl" => Ok(JoinMethod::DamerauLevenshtein { max_distance }),
        "hamming" => Ok(JoinMethod::Hamming { max_distance }),
        "lcs" => Ok(JoinMethod::LCS { max_distance }),
        "qgram" => Ok(JoinMethod::QGram {
            max_distance,
            q: q.ok_or_else(|| anyhow::anyhow!("Must provide `q` for method `qgram`"))?,
        }),
        "cosine" => Ok(JoinMethod::Cosine {
            max_distance,
            q: q.ok_or_else(|| anyhow::anyhow!("Must provide `q` for method `cosine`"))?,
        }),
        "jaccard" => Ok(JoinMethod::Jaccard {
            max_distance,
            q: q.ok_or_else(|| anyhow::anyhow!("Must provide `q` for method `jaccard`"))?,
        }),
        "jaro_winkler" | "jw" => Ok(JoinMethod::JaroWinkler {
            max_distance,
            prefix_weight: prefix_weight
                .ok_or_else(|| anyhow::anyhow!("Must provide `prefix_weight`"))?,
            max_prefix: max_prefix.ok_or_else(|| anyhow::anyhow!("Must provide `max_prefix`"))?,
        }),
        "soundex" => Ok(JoinMethod::Soundex {}),

        _ => Err(anyhow::anyhow!("Unsupported method `{}`", method)),
    }
}
