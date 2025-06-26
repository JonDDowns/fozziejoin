// All text distance algorithms either directly use or import the
// the `textdistance` crate by orsinium.
// Source: https://docs.rs/textdistance/latest/textdistance/
// License: MIT

use crate::normalized::NormalizedEditDistance;
use textdistance::{Algorithm, JaroWinkler as TDJaroWinkler};

pub struct JaroWinkler {
    alg: TDJaroWinkler,
}

impl JaroWinkler {
    pub fn default() -> Self {
        Self {
            alg: TDJaroWinkler::default(),
        }
    }

    pub fn new(prefix_weight: f64, max_prefix: usize) -> Self {
        let mut alg = TDJaroWinkler::default();
        alg.max_prefix = max_prefix;
        alg.prefix_weight = prefix_weight;
        Self { alg }
    }
}

impl NormalizedEditDistance for JaroWinkler {
    fn compute(&self, s1: &str, s2: &str) -> f64 {
        self.alg.for_str(s1, s2).ndist()
    }
}
