#' @note
#' This function draws inspiration from the `fuzzyjoin` package, particularly in its flexible handling of the `by` argument.
#' It supports character vectors, named lists, and automatic detection of shared column names when `by` is not specified.
"%||%" <- function(x, y) if (is.null(x)) y else x

#' Normalize Join Columns
#'
#' Join columns expect a named list, where names are left-hand columns to join
#' on, and values are right-hand columns to join on. This function ensures a
#' fuzzy-like syntax to the user while producing the correct output for the
#' Rust join utilities.
#'
#' @param df1 A data frame representing the left-hand side of the join.
#' @param df2 A data frame representing the right-hand side of the join.
#' @param by A named list or character vector specifying join columns. If NULL,
#'   shared column names between df1 and df2 are used.
#'
#' @return A named list mapping left-hand columns to right-hand columns.
#' @export
normalize_by <- function(df1, df2, by) {
  # If no by provided, identify shared column names
  if (is.null(by)) {
    shared <- intersect(names(df1), names(df2))
    if (length(shared) == 0) {
      stop("No shared column names found between df1 and df2.",
            "Please specify join columns using the `by` parameter.")
    }
    message("Joining by: ", utils::capture.output(dput(shared)))
    return(setNames(as.list(shared), shared))
  }

  # Handle partially unnamed vec/list- assume unnamed values
  # mean the column name is the same in left and right
  x <- names(by) %||% by
  y <- unname(by)
  x[x == ""] <- y[x == ""]

  # If any columns are not in their expected dfs, that's a problem
  invalid_x <- setdiff(x, colnames(df1))
  invalid_y <- setdiff(y, colnames(df2))
  if (length(invalid_x) > 0) {
    stop(paste("The following columns are not in the left dataframe:", invalid_x))
  }
  if (length(invalid_y) > 0) {
    stop(paste("The following columns are not in the right dataframe:", invalid_y))
  }

  return(setNames(as.list(y), x))
}

convert_output <- function(left, right, out) {
  is_tibble_input <- inherits(left, "tbl_df") || inherits(right, "tbl_df")
  if (is_tibble_input) {
    result <- tibble::as_tibble(out)
  } else {
    result <- as.data.frame(out)
  }
  result
}
