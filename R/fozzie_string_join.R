#' Perform a fuzzy join between two data frames using approximate string matching.
#'
#' `fozzie_string_join()` and its directional variants (`fozzie_string_inner_join()`, `fozzie_string_left_join()`, `fozzie_string_right_join()`, `fozzie_string_anti_join()`, `fozzie_string_full_join()`)
#' enable approximate matching of string fields in two data frames. These joins support multiple string distance
#' and similarity algorithms including Levenshtein, Jaro-Winkler, q-gram similarity, and others.
#'
#' @param df1 A data frame to join from (left table).
#' @param df2 A data frame to join to (right table).
#' @param by A named list or character vector indicating the matching columns. Can be a character vector of length 2, e.g. `c("col1", "col2")`,
#'   or a named list like `list(col1 = "col2")`.
#' @param method A string indicating the fuzzy matching method. Supported methods:
#'   - `"levenshtein"`: Levenshtein edit distance (default).
#'   - `"osa"`: Optimal string alignment.
#'   - `"damerau_levensthein"` or `"dl"`: Damerau-Levenshtein distance.
#'   - `"hamming"`: Hamming distance (equal-length strings only).
#'   - `"lcs"`: Longest common subsequence.
#'   - `"qgram"`: Q-gram similarity (requires `q`).
#'   - `"cosine"`: Cosine similarity (requires `q`).
#'   - `"jaccard"`: Jaccard similarity (requires `q`).
#'   - `"jaro"`: Jaro similarity.
#'   - `"jaro_winkler"` or `"jw"`: Jaro-Winkler similarity.
#'   - `"soundex"`: Soundex codes based on the National Archives standard.
#' @param how A string specifying the join mode. One of:
#'   - `"inner"`: matched pairs only.
#'   - `"left"`: all rows from `df1`, unmatched rows filled with NAs.
#'   - `"right"`: all rows from `df2`, unmatched rows filled with NAs.
#'   - `"full"`: all rows from both `df1` and `df2`.
#'   - `"anti"`: rows from `df1` not matched in `df2`.
#'   - `"semi"`: rows from `df1` that matched with one or more matches in `df2`.
#' @param q Integer. Size of q-grams for `"qgram"`, `"cosine"`, or `"jaccard"` methods.
#' @param max_distance A numeric threshold for allowable string distance or dissimilarity (lower is stricter).
#' @param distance_col Optional name of column to store computed string distances.
#' @param max_prefix Integer (for Jaro-Winkler) specifying the prefix length influencing similarity boost.
#' @param prefix_weight Numeric (for Jaro-Winkler) specifying the prefix weighting factor.
#' @param nthread Optional integer specifying the number of threads to use for
#'        parallelization. If not provided, the value is determined by 
#'        `options("fozzie.nthread")`. The package default is inherited from
#'        Rayon, the multithreading library used throughout the package.
#'
#' @return A data frame with fuzzy-matched rows depending on the join type. See individual functions like `fozzie_string_inner_join()` for examples.
#'   If `distance_col` is specified, an additional numeric column is included.
#'
#' @examples
#' df1 <- data.frame(name = c("Alice", "Bob", "Charlie"))
#' df2 <- data.frame(name = c("Alicia", "Robert", "Charles"))
#'
#' fozzie_string_inner_join(
#'   df1, df2, by = c("name"), method = "levenshtein", max_distance = 2
#' )
#' fozzie_string_left_join(
#'   df1, df2, by = c("name"), method = "jw", max_distance = 0.2
#' )
#' fozzie_string_right_join(
#'   df1, df2, by = c("name"), method = "cosine", q = 2, max_distance = 0.1
#'  )
#'
#' @name fozzie_string_join_family
#' @export
fozzie_string_join <- function(
    df1, df2, by = NULL,
    method = "levenshtein",
    how = "inner",
    max_distance = 1,
    distance_col = NULL,
    q = NULL,
    max_prefix = 0,
    prefix_weight = 0,
    nthread = getOption("fozzie.nthread", NULL)) {
  by <- normalize_by(df1, df2, by)

  # Run Rust function and return
  tmp <- fozzie_string_join_rs(
    df1, df2, by, method, how,
    max_distance, distance_col, q, max_prefix, prefix_weight, nthread
  )
  convert_output(df1, df2, tmp)
}

#' @rdname fozzie_string_join_family
#' @return See [fozzie_string_join()]
#' @export
fozzie_string_inner_join <- function(
    df1, df2, by = NULL,
    method = "levenshtein",
    max_distance = 1,
    distance_col = NULL,
    q = NULL,
    max_prefix = 0,
    prefix_weight = 0,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_string_join(
    df1, df2, by,
    method = method,
    max_distance = max_distance,
    distance_col = distance_col,
    q = q,
    max_prefix = max_prefix,
    prefix_weight = prefix_weight,
    nthread = nthread,
    how = "inner"
  )
}

#' @rdname fozzie_string_join_family
#' @return See [fozzie_string_join()]
#' @export
fozzie_string_left_join <- function(
    df1, df2, by = NULL,
    method = "levenshtein",
    max_distance = 1,
    distance_col = NULL,
    q = NULL,
    max_prefix = 0,
    prefix_weight = 0,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_string_join(
    df1, df2, by,
    method = method,
    max_distance = max_distance,
    distance_col = distance_col,
    q = q,
    max_prefix = max_prefix,
    prefix_weight = prefix_weight,
    nthread = nthread,
    how = "left"
  )
}

#' @rdname fozzie_string_join_family
#' @return See [fozzie_string_join()]
#' @export
fozzie_string_right_join <- function(
    df1, df2, by = NULL,
    method = "levenshtein",
    max_distance = 1,
    distance_col = NULL,
    q = NULL,
    max_prefix = 0,
    prefix_weight = 0,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_string_join(
    df1, df2, by,
    method = method,
    max_distance = max_distance,
    distance_col = distance_col,
    q = q,
    max_prefix = max_prefix,
    prefix_weight = prefix_weight,
    nthread = nthread,
    how = "right"
  )
}

#' @rdname fozzie_string_join_family
#' @return See [fozzie_string_join()]
#' @export
fozzie_string_anti_join <- function(
    df1, df2, by = NULL,
    method = "levenshtein",
    max_distance = 1,
    distance_col = NULL,
    q = NULL,
    max_prefix = 0,
    prefix_weight = 0,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_string_join(
    df1, df2, by,
    method = method,
    max_distance = max_distance,
    distance_col = distance_col,
    q = q,
    max_prefix = max_prefix,
    prefix_weight = prefix_weight,
    nthread = nthread,
    how = "anti"
  )
}

#' @rdname fozzie_string_join_family
#' @return See [fozzie_string_join()]
#' @export
fozzie_string_full_join <- function(
    df1, df2, by = NULL,
    method = "levenshtein",
    max_distance = 1,
    distance_col = NULL,
    q = NULL,
    max_prefix = 0,
    prefix_weight = 0,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_string_join(
    df1, df2, by,
    method = method,
    max_distance = max_distance,
    distance_col = distance_col,
    q = q,
    max_prefix = max_prefix,
    prefix_weight = prefix_weight,
    nthread = nthread,
    how = "full"
  )
}

#' @rdname fozzie_string_join_family
#' @return See [fozzie_string_join()]
#' @export
fozzie_string_semi_join <- function(
    df1, df2, by = NULL,
    method = "levenshtein",
    max_distance = 1,
    distance_col = NULL,
    q = NULL,
    max_prefix = 0,
    prefix_weight = 0,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_string_join(
    df1, df2, by,
    method = method,
    max_distance = max_distance,
    distance_col = distance_col,
    q = q,
    max_prefix = max_prefix,
    prefix_weight = prefix_weight,
    nthread = nthread,
    how = "semi"
  )
}
