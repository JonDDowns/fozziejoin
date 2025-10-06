#' Perform a fuzzy join between two data frames using interval overlap matching.
#'
#' `fozzie_interval_join()` and its directional variants (`fozzie_interval_inner_join()`, `fozzie_interval_left_join()`, etc.)
#' enable approximate matching of interval columns in two data frames based on overlap logic.
#' These joins are analogous to `data.table::foverlaps`, but implemented in Rust for performance.
#'
#' @param df1 A data frame to join from (left table).
#' @param df2 A data frame to join to (right table).
#' @param by A named list mapping left and right interval columns. Must contain two entries: start and end.
#' @param how A string specifying the join mode. One of:
#'   - `"inner"`: matched pairs only.
#'   - `"left"`: all rows from `df1`, unmatched rows filled with NAs.
#'   - `"right"`: all rows from `df2`, unmatched rows filled with NAs.
#'   - `"full"`: all rows from both `df1` and `df2`.
#'   - `"anti"`: rows from `df1` not matched in `df2`.
#'   - `"semi"`: rows from `df1` that matched with one or more matches in `df2`.
#' @param overlap_type A string specifying the overlap logic. One of:
#'   - `"any"`: any overlap.
#'   - `"within"`: left interval fully within right.
#'   - `"start"`: left start within right.
#'   - `"end"`: left end within right.
#' @param maxgap Maximum allowed gap between intervals (non-negative).
#' @param minoverlap Minimum required overlap length (non-negative).
#' @param nthread Optional integer to specify number of threads for parallelization.
#'
#' @return A data frame with approximately matched rows depending on the join type.
#'
#' @examples
#' df1 <- data.frame(start = c(1, 5), end = c(3, 7))
#' df2 <- data.frame(start = c(2, 6), end = c(4, 8))
#'
#' fozzie_interval_inner_join(df1, df2, by = list(start = "start", end = "end"), overlap_type = "any")
#'
#' @name fozzie_interval_join_family
#' @export
fozzie_interval_join <- function(
    df1, df2, by = NULL,
    how = "inner",
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    interval_mode = c("auto", "real", "integer"),
    nthread = NULL) {
  by <- normalize_by(df1, df2, by)

  interval_mode <- match.arg(interval_mode)

  if (interval_mode == "auto") {
    # Infer mode based on column types
    all_cols <- c(names(by), unlist(by))
    all_types <- sapply(all_cols, function(col) {
      c(typeof(df1[[col]]), typeof(df2[[col]]))
    })
    if (all(all_types == "integer")) {
      interval_mode <- "integer"
    } else {
      interval_mode <- "real"
    }
  }

  tmp <- fozzie_interval_join_rs(
    df1, df2, by,
    how = how,
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    interval_mode = interval_mode,
    nthread = nthread
  )
  data.frame(tmp)
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_inner_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    interval_mode = "auto",
    nthread = NULL) {
  fozzie_interval_join(
    df1, df2, by,
    how = "inner",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    interval_mode = interval_mode,
    nthread = nthread
  )
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_left_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    interval_mode = "auto",
    nthread = NULL) {
  fozzie_interval_join(
    df1, df2, by,
    how = "left",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    interval_mode = interval_mode,
    nthread = nthread
  )
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_right_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    interval_mode = "auto",
    nthread = NULL) {
  fozzie_interval_join(
    df1, df2, by,
    how = "right",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    interval_mode = interval_mode,
    nthread = nthread
  )
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_full_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    interval_mode = "auto",
    nthread = NULL) {
  fozzie_interval_join(
    df1, df2, by,
    how = "full",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    interval_mode = interval_mode,
    nthread = nthread
  )
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_anti_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    interval_mode = "auto",
    nthread = NULL) {
  fozzie_interval_join(
    df1, df2, by,
    how = "anti",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    interval_mode = interval_mode,
    nthread = nthread
  )
}

#' @rdname fozzie_interval_join_family
#' @export
fozzie_interval_semi_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    interval_mode = "auto",
    nthread = NULL) {
  fozzie_interval_join(
    df1, df2, by,
    how = "semi",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    interval_mode = interval_mode,
    nthread = nthread
  )
}
