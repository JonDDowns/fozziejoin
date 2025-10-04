use extendr_api::prelude::*;

pub struct Merge;
pub mod anti;
pub mod full;
pub mod inner;
pub mod left;
pub mod right;
pub mod semi;

pub fn dispatch_join(
    how: &str,
    df1: &List,
    df2: &List,
    idxs1: Vec<usize>,
    idxs2: Vec<usize>,
    distance_col: Option<String>,
    dist: DistanceData,
    by: List,
) -> List {
    match how {
        "inner" => Merge::inner(df1, df2, idxs1, idxs2, distance_col, dist, by),
        "left" => Merge::left(df1, df2, idxs1, idxs2, distance_col, dist, by),
        "right" => Merge::right(df1, df2, idxs1, idxs2, distance_col, dist, by),
        "full" => Merge::full(df1, df2, idxs1, idxs2, distance_col, dist, by),
        "anti" => Merge::anti(df1, idxs1),
        "semi" => Merge::semi(df1, idxs1),
        _ => panic!("Unknown join type: {}", how),
    }
}

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

/// Helper to subset and label columns from a data frame
pub fn subset_and_label(df: &List, indices: &[usize]) -> (Vec<String>, Vec<Robj>) {
    let mut names = Vec::with_capacity(df.ncols());
    let mut columns = Vec::with_capacity(df.ncols());
    for (name, col) in df.iter() {
        let vals = col.slice(indices).unwrap();
        names.push(name.to_string());
        columns.push(vals);
    }
    (names, columns)
}

/// Helper to construct distance columns
pub fn build_distance_columns(
    dist: &[Vec<f64>],
    by: &List,
    distance_col: &str,
) -> (Vec<String>, Vec<Robj>) {
    let mut names = Vec::with_capacity(dist.len());
    let mut columns = Vec::with_capacity(dist.len());

    let ndist = dist.len();
    for (x, (y, z)) in dist.iter().zip(by.iter()) {
        let cname = if ndist == 1 {
            distance_col.to_string()
        } else {
            format!(
                "{}_{}_{}",
                distance_col,
                y,
                z.as_str_vector().expect("hi")[0]
            )
        };
        names.push(cname);
        columns.push(x.into_robj());
    }

    (names, columns)
}

/// Helper to construct single distance column
pub fn build_single_distance_column(dist: &[f64], distance_col: &str) -> (String, Robj) {
    (distance_col.to_string(), dist.to_vec().into_robj())
}

/// Pad a column with R-style NA values based on its type
pub fn pad_column(col: &Robj, pad_len: usize) -> Robj {
    match col.rtype() {
        Rtype::Integers => Robj::from(vec![Rint::na(); pad_len]),
        Rtype::Doubles => Robj::from(vec![Rfloat::na(); pad_len]),
        Rtype::Logicals => Robj::from(vec![Rbool::na(); pad_len]),
        Rtype::Strings => Robj::from(vec![Rstr::na(); pad_len]),
        _ => Robj::from(vec![Robj::from(()); pad_len]),
    }
}

pub enum DistanceData<'a> {
    Single(&'a Vec<f64>),
    Matrix(&'a Vec<Vec<f64>>),
}
