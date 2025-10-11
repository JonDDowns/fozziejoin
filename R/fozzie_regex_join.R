#' Perform a fuzzy join between two data frames using regex pattern matching.
#'
#' `fozzie_regex_join()` and its directional variants (`fozzie_regex_inner_join()`, `fozzie_regex_left_join()`, `fozzie_regex_right_join()`, `fozzie_regex_anti_join()`, `fozzie_regex_full_join()`, `fozzie_regex_semi_join()`)
#' enable approximate matching of string fields in two data frames using regular expressions.
#' These joins are analogous to `fuzzyjoin::regex_join`, but implemented in Rust for performance.
#' 
#' The right-hand column (from `df2`) is treated as a vector of regex patterns, and each value in the left-hand column (from `df1`) is matched against those patterns.
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
#' @param ignore_case Should be case insensitive. Default is FALSE.
#' @param nthread Optional integer specifying the number of threads to use for
#'        parallelization. If not provided, the value is determined by 
#'        `options("fozzie.nthread")`. The package default is inherited from
#'         Rayon, the multithreading library used throughout the package.
#' @return A data frame with approximately matched rows depending on the join type. See individual functions like `fozzie_regex_inner_join()` for examples.
#'
#' @examples
#' df1 <- data.frame(name = c("apple", "banana", "cherry"))
#' df2 <- data.frame(pattern = c("^a", "an", "rry$"))
#'
#' fozzie_regex_inner_join(df1, df2, by = c("name" = "pattern"))
#' fozzie_regex_left_join(df1, df2, by = c("name" =  "pattern"))
#'
#' @name fozzie_regex_join_family
#' @export
fozzie_regex_join <- function(
    df1, df2, by = NULL,
    how = "inner",
    ignore_case = FALSE,
    nthread = getOption("fozzie.nthread", NULL)) {
  by <- normalize_by(df1, df2, by)
  tmp <- fozzie_regex_join_rs(
    df1, df2, by,
    how = how,
    ignore_case = ignore_case,
    nthread = nthread
  )
  convert_output(df1, df2, tmp)
}

#' @rdname fozzie_regex_join_family
#' @export
fozzie_regex_inner_join <- function(
    df1, df2, by = NULL,
    ignore_case = FALSE,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_regex_join(
    df1, df2, by,
    how = "inner",
    ignore_case = ignore_case,
    nthread = nthread
  )
}

#' @rdname fozzie_regex_join_family
#' @export
fozzie_regex_left_join <- function(
    df1, df2, by = NULL,
    ignore_case = FALSE,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_regex_join(
    df1, df2, by,
    how = "left",
    ignore_case = ignore_case,
    nthread = nthread
  )
}

#' @rdname fozzie_regex_join_family
#' @export
fozzie_regex_right_join <- function(
    df1, df2, by = NULL,
    ignore_case = FALSE,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_regex_join(
    df1, df2, by,
    how = "right",
    ignore_case = ignore_case,
    nthread = nthread
  )
}

#' @rdname fozzie_regex_join_family
#' @export
fozzie_regex_anti_join <- function(
    df1, df2, by = NULL,
    ignore_case = FALSE,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_regex_join(
    df1, df2, by,
    how = "anti",
    ignore_case = ignore_case,
    nthread = nthread
  )
}

#' @rdname fozzie_regex_join_family
#' @export
fozzie_regex_full_join <- function(
    df1, df2, by = NULL,
    ignore_case = FALSE,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_regex_join(
    df1, df2, by,
    how = "full",
    ignore_case = ignore_case,
    nthread = nthread
  )
}

#' @rdname fozzie_regex_join_family
#' @export
fozzie_regex_semi_join <- function(
    df1, df2, by = NULL,
    ignore_case = FALSE,
    nthread = getOption("fozzie.nthread", NULL)) {
  fozzie_regex_join(
    df1, df2, by,
    how = "semi",
    ignore_case = ignore_case,
    nthread = nthread
  )
}

