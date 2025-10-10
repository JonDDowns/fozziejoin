#' Perform a fuzzy join between two data frames using time-based interval overlap matching.
#'
#' `fozzie_temporal_interval_join()` and its directional variants (`fozzie_temporal_interval_inner_join()`, `fozzie_temporal_interval_left_join()`, etc.)
#' enable approximate matching of time-based intervals in two data frames using continuous overlap logic.
#'
#' All interval columns must be of the same type — either `Date` or `POSIXct` — across both data frames. Mixed types are not supported. Overlaps are computed using real-valued time semantics, allowing for fractional gaps and overlaps. This is useful for calendar intervals (`Date`) as well as precise timestamp ranges (`POSIXct`).
#'
#' @param df1 A data frame to join from (left table).
#' @param df2 A data frame to join to (right table).
#' @param by A named list mapping left and right interval columns. Must contain two entries: `start` and `end`.
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
#' @param maxgap Maximum allowed gap between intervals, expressed in the specified time unit.
#' @param minoverlap Minimum required overlap length, expressed in the specified time unit.
#' @param unit A string specifying the time unit for `maxgap` and `minoverlap`. One of:
#'   `"days"`, `"hours"`, `"minutes"`, `"seconds"`, `"ms"`, `"us"`, `"ns"`.
#' @param nthread Optional integer specifying the number of threads to use for
#'        parallelization. If not provided, the value is determined by 
#'        `options("fozzie.nthread")`. The package default is inherited from
#'        Rayon, the multithreading library used throughout the package.
#'
#' @return A data frame with approximately matched rows depending on the join type.
#'
#' @examples
#' df1 <- data.frame(
#'   start = as.Date(c("2023-01-01", "2023-01-05")),
#'   end = as.Date(c("2023-01-03", "2023-01-07"))
#' )
#' df2 <- data.frame(
#'   start = as.Date(c("2023-01-02", "2023-01-06")),
#'   end = as.Date(c("2023-01-04", "2023-01-08"))
#' )
#'
#' fozzie_temporal_interval_inner_join(
#'   df1, df2,
#'   by = list(start = "start", end = "end"),
#'   overlap_type = "any",
#'   maxgap = 0.5,
#'   unit = "days"
#' )
#'
#' @name fozzie_temporal_interval_join_family
#' @export
fozzie_temporal_interval_join <- function(
    df1, df2, by = NULL,
    how = "inner",
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    nthread = getOption("fozzie.nthread", NULL)) {
  unit <- match.arg(unit)
  by <- normalize_by(df1, df2, by)

  # Validate join columns and enforce consistent temporal types
  left_classes <- c()
  right_classes <- c()

  for (key in names(by)) {
    col1 <- df1[[key]]
    col2 <- df2[[by[[key]]]]

    if (inherits(col1, "POSIXlt") || inherits(col2, "POSIXlt")) {
      stop(sprintf("Column '%s' uses POSIXlt, which is not supported. Please convert to POSIXct or Date.", key))
    }

    if (!(inherits(col1, "POSIXct") || inherits(col1, "Date")) ||
      !(inherits(col2, "POSIXct") || inherits(col2, "Date"))) {
      stop(sprintf("Column '%s' must be of class 'Date' or 'POSIXct' in both data frames.", key))
    }

    left_classes <- c(left_classes, class(col1)[1])
    right_classes <- c(right_classes, class(col2)[1])
  }

  # Check for consistent types across all columns
  if (!all(left_classes == left_classes[1]) || !all(right_classes == right_classes[1])) {
    stop("All join columns must be of the same type within each data frame.")
  }
  if (left_classes[1] != right_classes[1]) {
    stop("Join columns must be of the same type across both data frames (either all Date or all POSIXct).")
  }

  # Convert gap and overlap to seconds
  if (left_classes[1] == "Date") {
    if (unit != "days") {
      stop("When joining on Date columns, unit must be 'days'.")
    }
    maxgap_final <- maxgap
    minoverlap_final <- minoverlap
  } else {
    unit_multipliers <- c(
      ns = 1e-9, us = 1e-6, ms = 1e-3,
      seconds = 1, minutes = 60, hours = 3600,
      days = 86400
    )
    maxgap_final <- maxgap * unit_multipliers[[unit]]
    minoverlap_final <- minoverlap * unit_multipliers[[unit]]
  }

  tmp <- fozzie_interval_join_rs(
    df1, df2, by,
    how = how,
    overlap_type = overlap_type,
    maxgap = maxgap_final,
    minoverlap = minoverlap_final,
    interval_mode = "real",
    nthread = nthread
  )
  convert_output(df1, df2, tmp)
}

#' @rdname fozzie_temporal_interval_join_family
#' @export
fozzie_temporal_interval_inner_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_interval_join(
    df1, df2, by,
    how = "inner",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    unit = unit,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_interval_join_family
#' @export
fozzie_temporal_interval_left_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_interval_join(
    df1, df2, by,
    how = "left",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    unit = unit,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_interval_join_family
#' @export
fozzie_temporal_interval_right_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_interval_join(
    df1, df2, by,
    how = "right",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    unit = unit,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_interval_join_family
#' @export
fozzie_temporal_interval_full_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_interval_join(
    df1, df2, by,
    how = "full",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    unit = unit,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_interval_join_family
#' @export
fozzie_temporal_interval_anti_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_interval_join(
    df1, df2, by,
    how = "anti",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    unit = unit,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_interval_join_family
#' @export
fozzie_temporal_interval_semi_join <- function(
    df1, df2, by = NULL,
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_interval_join(
    df1, df2, by,
    how = "semi",
    overlap_type = overlap_type,
    maxgap = maxgap,
    minoverlap = minoverlap,
    unit = unit,
    nthread = nthread
  )
}
