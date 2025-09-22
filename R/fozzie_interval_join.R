#' Perform a fuzzy join between two data frames using interval containment.
#'
#' `fozzie_interval_join()` and its directional variants (`fozzie_interval_inner_join()`, `fozzie_interval_left_join()`, `fozzie_interval_right_join()`, `fozzie_interval_anti_join()`, `fozzie_interval_full_join()`)
#' enable approximate matching of numeric fields in two data frames based on interval containment logic.
#' These joins are analogous to `fuzzyjoin::interval_join`, but implemented in Rust for performance.
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
#' @param max_distance A numeric threshold for allowable interval expansion (optional).
#' @param distance_col Optional name of column to store computed distances.
#' @param nthread Optional integer to specify number of threads for parallelization.
#'
#' @return A data frame with approximately matched rows depending on the join type.
#'
#' @examples
#' df1 <- data.frame(x = c(1.5, 2.5, 3.5))
#' df2 <- data.frame(lower = c(1.0, 2.0, 3.0), upper = c(2.0, 3.0, 4.0))
#'
#' fozzie_interval_inner_join(df1, df2, by = c("x", "lower"), max_distance = 0.1)
#'
#' @name fozzie_interval_join_family
#' @export
fozzie_interval_join <- function(df1, df2, by, how = "inner", max_distance = 1, distance_col = NULL, nthread = NULL) {
  warning("Interval joins are still experimental. Proceed with caution, and please report any issues!")
  if (is.character(by) && length(by) == 2) {
    by <- setNames(list(by[2]), by[1])
  }
  fozzie_interval_join_rs(
    df1, df2, by,
    how = how,
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_inner_join <- function(df1, df2, by, max_distance = 1, distance_col = NULL, nthread = NULL) {
  fozzie_interval_join(df1, df2, by, how = "inner", max_distance = max_distance, distance_col = distance_col, nthread = nthread)
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_left_join <- function(df1, df2, by, max_distance = 1, distance_col = NULL, nthread = NULL) {
  fozzie_interval_join(df1, df2, by, how = "left", max_distance = max_distance, distance_col = distance_col, nthread = nthread)
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_right_join <- function(df1, df2, by, max_distance = 1, distance_col = NULL, nthread = NULL) {
  fozzie_interval_join(df1, df2, by, how = "right", max_distance = max_distance, distance_col = distance_col, nthread = nthread)
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_anti_join <- function(df1, df2, by, max_distance = 1, distance_col = NULL, nthread = NULL) {
  fozzie_interval_join(df1, df2, by, how = "anti", max_distance = max_distance, distance_col = distance_col, nthread = nthread)
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_full_join <- function(df1, df2, by, max_distance = 1, distance_col = NULL, nthread = NULL) {
  fozzie_interval_join(df1, df2, by, how = "full", max_distance = max_distance, distance_col = distance_col, nthread = nthread)
}

