#' @note
#' This function draws inspiration from the `fuzzyjoin` package, particularly in its flexible handling of the `by` argument.
#' It supports character vectors, named lists, and automatic detection of shared column names when `by` is not specified.
"%||%" <- function(x, y) if (is.null(x)) y else x

#' Join columns expect a named list, where names are left-hand columns to join
#' on, and values are right-hand columns to join on. This function ensures a
#' fuzzy-like syntax to the user while producing the correct output for the
#' rust join utilities.
normalize_by <- function(df1, df2, by) {
  # If no by provided, identify shared column names
  if (is.null(by)) {
    shared <- intersect(names(df1), names(df2))
    if (length(shared) == 0) {
      stop("No shared column names found between df1 and df2.")
    }
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
