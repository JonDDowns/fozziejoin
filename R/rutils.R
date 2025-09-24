#' @note
#' This function draws inspiration from the `fuzzyjoin` package, particularly in its flexible handling of the `by` argument.
#' It supports character vectors, named lists, and automatic detection of shared column names when `by` is not specified.
normalize_by <- function(df1, df2, by) {
  # If `by` is NULL, auto-detect shared columns
  if (is.null(by)) {
    shared <- intersect(names(df1), names(df2))
    if (length(shared) == 0) {
      stop("No shared column names found between df1 and df2.")
    }
    by <- setNames(as.list(shared), shared)
    return(by)
  }

  # If `by` is a named character vector (e.g. c("x1" = "x2"))
  if (is.character(by) && !is.null(names(by)) && all(names(by) != "")) {
    return(as.list(by))
  }

  # If `by` is a character vector of length 1 or 2
  if (is.character(by)) {
    if (length(by) == 1) {
      return(setNames(list(by), by))
    } else if (length(by) == 2) {
      return(setNames(list(by[2]), by[1]))
    } else {
      stop("Character `by` must be length 1 or 2.")
    }
  }

  # If `by` is a list, fill in missing names
  if (is.list(by)) {
    if (is.null(names(by)) || any(names(by) == "")) {
      names(by) <- vapply(by, as.character, character(1))
    }
    return(by)
  }

  stop("Invalid `by` argument.")
}
