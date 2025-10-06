pub mod integer;
pub mod real;

use anyhow::{anyhow, Result};

#[derive(Debug, Clone, Copy)]
pub enum OverlapType {
    Any,
    Within,
    Start,
    End,
}

impl OverlapType {
    pub fn new(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "any" => Ok(OverlapType::Any),
            "within" => Ok(OverlapType::Within),
            "start" => Ok(OverlapType::Start),
            "end" => Ok(OverlapType::End),
            _ => Err(anyhow!("Unknown overlap type: {}", s)),
        }
    }
}
