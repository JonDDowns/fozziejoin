#' Perform a fuzzy join between two data frames using temporal difference matching.
#'
#' `fozzie_temporal_join()` and its directional variants (`fozzie_temporal_inner_join()`, `fozzie_temporal_left_join()`, etc.)
#' enable approximate matching of temporal columns in two data frames based on absolute time difference thresholds.
#' These joins are conceptually similar to `fozzie_difference_join()`, but specialized for temporal data types (`Date` and `POSIXct`).
#'
#' All join columns must be either `Date` or `POSIXct`, and must be consistent across both data frames. Mixed types (e.g., `Date` in one and `POSIXct` in the other) are not allowed.
#'
#' @param df1 A data frame to join from (left table).
#' @param df2 A data frame to join to (right table).
#' @param by A named list indicating the matching temporal columns, e.g. `list(time1 = "time2")`.
#' @param how A string specifying the join mode. One of:
#'   - `"inner"`: matched pairs only.
#'   - `"left"`: all rows from `df1`, unmatched rows filled with NAs.
#'   - `"right"`: all rows from `df2`, unmatched rows filled with NAs.
#'   - `"full"`: all rows from both `df1` and `df2`.
#'   - `"anti"`: rows from `df1` not matched in `df2`.
#'   - `"semi"`: rows from `df1` that matched with one or more matches in `df2`.
#' @param max_distance Maximum allowed time difference between values.
#' @param unit A string specifying the time unit for `max_distance`. One of:
#'   `"days"`, `"hours"`, `"minutes"`, `"seconds"`, `"ms"`, `"us"`, `"ns"`.
#'   If joining on `Date` columns, only `"days"` is allowed.
#' @param distance_col Optional name of column to store computed time differences (in seconds or days).
#' @param nthread Optional integer specifying the number of threads to use for
#'        parallelization. If not provided, the value is determined by 
#'        `options("fozzie.nthread")`. The package default is inherited from
#'        Rayon, the multithreading library used throughout the package.
#'
#' @return A data frame with approximately matched rows depending on the join type. If `distance_col` is specified, an additional numeric column is included.
#'
#' @examples
#' df1 <- data.frame(time = as.POSIXct(c("2023-01-01 12:00:00", "2023-01-01 13:00:00")))
#' df2 <- data.frame(time = as.POSIXct(c("2023-01-01 12:00:05", "2023-01-01 14:00:00")))
#'
#' fozzie_temporal_inner_join(df1, df2, by = list(time = "time"), max_distance = 10, unit = "seconds")
#'
#' df1 <- data.frame(date = as.Date(c("2023-01-01", "2023-01-03")))
#' df2 <- data.frame(date = as.Date(c("2023-01-02", "2023-01-04")))
#'
#' fozzie_temporal_inner_join(df1, df2, by = list(date = "date"), max_distance = 1, unit = "days")
#'
#' @name fozzie_temporal_join_family
#' @export
fozzie_temporal_join <- function(
    df1, df2,
    by = NULL,
    how = "inner",
    max_distance = 1,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    distance_col = NULL,
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

  # Determine mode and convert max_distance
  if (left_classes[1] == "Date") {
    if (unit != "days") {
      stop("When joining on Date columns, unit must be 'days'.")
    }
    max_distance_final <- max_distance # already in days
  } else {
    # Time unit multipliers to seconds
    unit_multipliers <- c(
      ns = 1e-9, us = 1e-6, ms = 1e-3,
      seconds = 1, minutes = 60, hours = 3600,
      days = 86400
    )
    max_distance_final <- max_distance * unit_multipliers[[unit]]
  }

  # Call core difference join
  result <- fozzie_difference_join_rs(
    df1, df2, by,
    how = how,
    max_distance = max_distance_final,
    distance_col = distance_col,
    nthread = nthread
  )

  convert_output(df1, df2, result)
}

#' @rdname fozzie_temporal_join_family
#' @export
fozzie_temporal_inner_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_join(
    df1, df2, by,
    how = "inner",
    max_distance = max_distance,
    unit = unit,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_join_family
#' @export
fozzie_temporal_left_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_join(
    df1, df2, by,
    how = "left",
    max_distance = max_distance,
    unit = unit,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_join_family
#' @export
fozzie_temporal_right_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_join(
    df1, df2, by,
    how = "right",
    max_distance = max_distance,
    unit = unit,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_join_family
#' @export
fozzie_temporal_full_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_join(
    df1, df2, by,
    how = "full",
    max_distance = max_distance,
    unit = unit,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_join_family
#' @export
fozzie_temporal_anti_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_join(
    df1, df2, by,
    how = "anti",
    max_distance = max_distance,
    unit = unit,
    distance_col = distance_col,
    nthread = nthread
  )
}

#' @rdname fozzie_temporal_join_family
#' @export
fozzie_temporal_semi_join <- function(
    df1, df2, by = NULL,
    max_distance = 1,
    unit = c("days", "hours", "minutes", "seconds", "ms", "us", "ns"),
    distance_col = NULL,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_temporal_join(
    df1, df2, by,
    how = "semi",
    max_distance = max_distance,
    unit = unit,
    distance_col = distance_col,
    nthread = nthread
  )
}
