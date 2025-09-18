use extendr_api::prelude::*;

pub struct Merge;
pub mod anti;
pub mod inner;
pub mod left;
pub mod right;

/// Combine two Robj vectors of the same type into one.
/// Preserves all attributes from `a`, including class, levels, label, etc.
pub fn combine_robj(a: &Robj, b: &Robj) -> Result<Robj> {
    // Ensure both inputs are of the same R type
    if a.rtype() != b.rtype() {
        return Err(Error::Other("Cannot combine: mismatched types".to_string()));
    }

    // Special case for list columns (e.g. POSIXlt or nested tibbles)
    if a.rtype() == Rtype::List {
        let list_a = a
            .as_list()
            .ok_or_else(|| Error::Other("Failed to parse list a".to_string()))?;
        let list_b = b
            .as_list()
            .ok_or_else(|| Error::Other("Failed to parse list b".to_string()))?;
        let merged = list_a
            .iter()
            .chain(list_b.iter())
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();
        let mut combined = List::from_values(merged).as_robj().clone();

        // Copy all attributes from `a`
        if let Some(attr_list) = a.get_attrib("attributes") {
            if let Some(attr_pairs) = attr_list.as_list() {
                for (key, val) in attr_pairs.iter() {
                    combined.set_attrib(key, val)?;
                }
            }
        }

        return Ok(combined);
    }

    // For atomic vectors, use R's native `c()` function
    let mut combined = call!("c", a, b)?;

    // Copy all attributes from `a`
    if let Some(attr_list) = a.get_attrib("attributes") {
        if let Some(attr_pairs) = attr_list.as_list() {
            for (key, val) in attr_pairs.iter() {
                combined.set_attrib(key, val)?;
            }
        }
    }

    Ok(combined)
}
