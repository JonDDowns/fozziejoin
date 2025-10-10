#' Perform a fuzzy join between two data frames using numeric difference matching.
#'
#' `fozzie_difference_join()` and its directional variants (`fozzie_difference_inner_join()`, `fozzie_difference_left_join()`, `fozzie_difference_right_join()`, `fozzie_difference_anti_join()`, `fozzie_difference_full_join()`)
#' enable approximate matching of numeric fields in two data frames based on absolute difference thresholds.
#' These joins are analogous to `fuzzyjoin::difference_join`, but implemented in Rust for performance.
#'
#' @param df1 A data frame to join from (left table).
#' @param df2 A data frame to join to (right table).
#' @param by A named list or character vector indicating the matching columns. Can be a character vector of length 2, e.g. `c("col1", "col2")`,
#'   or a named list like `list(col1 = "col2")`.
#' @param how A string specifying the join mode. One of:
#'   - `"inner"`: matched pairs only.
#'   - `"left"`: all rows from `df1`, unmatched rows filled with NAs.
#'   - `"right"`: all rows from `df2`, unmatched rows filled with NAs.
#'   - `"full"`: all rows from both `df1` and `df2`.
#'   - `"anti"`: rows from `df1` not matched in `df2`.
#'   - `"semi"`: rows from `df1` that matched with one or more matches in `df2`.
#' @param max_distance A numeric threshold for allowable absolute difference between values (lower is stricter).
#' @param distance_col Optional name of column to store computed differences.
#' @param nthread Optional integer specifying the number of threads to use for
#'        parallelization. If not provided, the value is determined by 
#'        `options("fozzie.nthread")`. The package default is inherited from
#'         Rayon, the multithreading library used throughout the package.
#' @return A data frame with approximately matched rows depending on the join type. See individual functions like `fozzie_difference_inner_join()` for examples.
#'   If `distance_col` is specified, an additional numeric column is included.
#'
#' @examples
#' df1 <- data.frame(x = c(1.0, 2.0, 3.0))
#' df2 <- data.frame(x = c(1.05, 2.1, 2.95))
#'
#' fozzie_difference_inner_join(df1, df2, by = c("x"), max_distance = 0.1)
#' fozzie_difference_left_join(df1, df2, by = c("x"), max_distance = 0.2)
#' fozzie_difference_right_join(df1, df2, by = c("x"), max_distance = 0.05)
#'
#' @name fozzie_difference_join_family
#' @export
fozzie_difference_join <- function(
    df1, df2, by = NULL,
    how = "inner",
    max_distance = 1,
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  by <- normalize_by(df1, df2, by)
  tmp <- fozzie_difference_join_rs(
    df1, df2, by,
    how = how,
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )
  convert_output(df1, df2, tmp)
}

#' @rdname fozzie_difference_join_family
#' @export
fozzie_difference_inner_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_difference_join(
    df1, df2, by,
    how = "inner",
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_difference_join_family
#' @export
fozzie_difference_left_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_difference_join(
    df1, df2, by,
    how = "left",
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_difference_join_family
#' @export
fozzie_difference_right_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_difference_join(
    df1, df2, by,
    how = "right",
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_difference_join_family
#' @export
fozzie_difference_anti_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_difference_join(
    df1, df2, by,
    how = "anti",
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_difference_join_family
#' @export
fozzie_difference_full_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_difference_join(
    df1, df2, by,
    how = "full",
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_difference_join_family
#' @export
fozzie_difference_semi_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_difference_join(
    df1, df2, by,
    how = "semi",
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )
}
