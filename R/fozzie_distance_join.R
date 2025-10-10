#' Perform a fuzzy join between two data frames using vector distance matching.
#'
#' `fozzie_distance_join()` and its directional variants (`fozzie_distance_inner_join()`, `fozzie_distance_left_join()`, `fozzie_distance_right_join()`, `fozzie_distance_anti_join()`, `fozzie_distance_full_join()`)
#' enable approximate matching of numeric fields in two data frames based on vector distance thresholds.
#' These joins are analogous to `fuzzyjoin::distance_join`, but implemented in Rust for performance.
#'
#' @param df1 A data frame to join from (left table).
#' @param df2 A data frame to join to (right table).
#' @param by A character vector of column names to match on. These columns must be numeric and present in both data frames.
#' @param how A string specifying the join mode. One of:
#'   - `"inner"`: matched pairs only.
#'   - `"left"`: all rows from `df1`, unmatched rows filled with NAs.
#'   - `"right"`: all rows from `df2`, unmatched rows filled with NAs.
#'   - `"full"`: all rows from both `df1` and `df2`.
#'   - `"anti"`: rows from `df1` not matched in `df2`.
#'   - `"semi"`: rows from `df1` that matched with one or more matches in `df2`.
#' @param max_distance A numeric threshold for allowable vector distance between rows.
#' @param method A string specifying the distance metric. One of:
#'   - `"manhattan"`: sum of absolute differences.
#'   - `"euclidean"`: square root of sum of squared differences.
#' @param distance_col Optional name of column to store computed distances.
#' @param nthread Optional integer specifying the number of threads to use for
#'        parallelization. If not provided, the value is determined by 
#'        `options("fozzie.nthread")`. The package default is inherited from
#'         Rayon, the multithreading library used throughout the package.
#'
#' @return A data frame with approximately matched rows depending on the join type. If `distance_col` is specified, an additional numeric column is included.
#'
#' @examples
#' df1 <- data.frame(x = c(1.0, 2.0), y = c(3.0, 4.0))
#' df2 <- data.frame(x = c(1.1, 2.1), y = c(3.1, 4.1))
#'
#' fozzie_distance_inner_join(df1, df2, by = c("x", "y"), max_distance = 0.3, method = "euclidean")
#'
#' @name fozzie_distance_join_family
#' @export
fozzie_distance_join <- function(
    df1, df2, by = NULL,
    how = "inner",
    max_distance = 1,
    method = "manhattan",
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  by <- normalize_by(df1, df2, by)
  tmp <- fozzie_distance_join_rs(
    df1, df2, by,
    how = how,
    max_distance = max_distance,
    method = method,
    distance_col = distance_col,
    nthread = nthread
  )
  convert_output(df1, df2, tmp)
}

#' @rdname fozzie_distance_join_family
#' @export
fozzie_distance_inner_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    method = "manhattan",
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_distance_join(
    df1, df2, by,
    how = "inner",
    max_distance = max_distance,
    method = method,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_distance_join_family
#' @export
fozzie_distance_left_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    method = "manhattan",
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_distance_join(
    df1, df2, by,
    how = "left",
    max_distance = max_distance,
    method = method,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_distance_join_family
#' @export
fozzie_distance_right_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    method = "manhattan",
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_distance_join(
    df1, df2, by,
    how = "right",
    max_distance = max_distance,
    method = method,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_distance_join_family
#' @export
fozzie_distance_full_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    method = "manhattan",
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_distance_join(
    df1, df2, by,
    how = "full",
    max_distance = max_distance,
    method = method,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_distance_join_family
#' @export
fozzie_distance_anti_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    method = "manhattan",
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_distance_join(
    df1, df2, by,
    how = "anti",
    max_distance = max_distance,
    method = method,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_distance_join_family
#' @export
fozzie_distance_semi_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    method = "manhattan",
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_distance_join(
    df1, df2, by,
    how = "semi",
    max_distance = max_distance,
    method = method,
    distance_col = distance_col,
    nthread = nthread
  )
}
