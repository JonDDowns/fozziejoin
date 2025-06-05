#' Perform a fuzzy join between two data frames using approximate string matching.
#'
#' This function matches records in `df1` and `df2` based on a specified column,
#' allowing fuzzy matches within a given distance threshold. It supports various
#' approximate matching methods including Levenshtein, Damerau-Levenshtein, Jaro-Winkler, and more.
#'
#' @param df1 A data frame containing the first dataset.
#' @param df2 A data frame containing the second dataset.
#' @param by A named vector specifying the columns to join on.
#'   - `names(by)[1]`: Column name in `df1`.
#'   - `names(by)[2]`: Corresponding column name in `df2`.
#' @param method String specifying the fuzzy matching algorithm to use. Options include:
#'   - `"levenshtein"` (default) - Standard Levenshtein edit distance.
#'   - `"osa"` - Optimal string alignment distance.
#'   - `"damerau_levensthein"` or `"dl"` - Damerau-Levenshtein edit distance.
#'   - `"hamming"` - Hamming distance (only works for equal-length strings).
#'   - `"lcs"` - Longest common subsequence.
#'   - `"qgram"` - Q-gram similarity (requires `q` parameter).
#'   - `"cosine"` - Cosine similarity (requires `q` parameter).
#'   - `"jaccard"` - Jaccard similarity (requires `q` parameter).
#'   - `"jaro_winkler"` or `"jw"` - Jaro-Winkler similarity.
#'   - `"jaro"` - Standard Jaro similarity.
#' @param q Integer specifying the *q*-gram size (only required for `"qgram"`, `"cosine"`, and `"jaccard"` methods).
#'   If `NULL`, an error will be raised for these methods.
#' @param max_distance Numeric value specifying the maximum allowable edit distance for a match.
#'
#' @return A data frame containing matched records from `df1` and `df2`,
#'   with column names suffixed as `.x` (from `df1`) and `.y` (from `df2`).
#'
#' @examples
#' df1 <- data.frame(Name = c("Alice", "Bob", "Charlie"))
#' df2 <- data.frame(Name = c("Alicia", "Robert", "Charles"))
#' result <- fozzie_join(df1, df2, by = list(Name = "Name"), method = "levenshtein", max_distance = 2)
#' print(result)
#'
#' @export
fozzie_join <- function(df1, df2, by, method='levenshtein', q=NULL, max_distance=1) {
  # Automatically convert character vector `by = c("COL1", "COL2")` into a named list
  if (is.character(by) && length(by) == 2) {
    by <- setNames(list(by[2]), by[1])
  }

  fozzie_join_rs(df1, df2, by, method, q, max_distance)
}

