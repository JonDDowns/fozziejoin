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
#' @param how A string specifying the type of join to perform (`"inner"`, `"left"`, `"right"`, or `"full"`).
#'   - `"inner"` (default): Returns only matches.
#'   - `"left"`: Returns all rows from `df1`, with matches from `df2`.
#'   - `"right"`: Returns all rows from `df2`, with matches from `df1`.
#'   - `"full"`: [Not implemented] Returns all rows from both `df1` and `df2`, matching where possible.
#'   - `"anti"`: [Not implemented] Returns all rows from `df1` not in `df2`.
#' @param distance_col Optional column name as a string to store the computed distance values in the output.
#'   If `NULL`, distances will not be included in the output.
#' @param p Numeric parameter used for certain distance calculations (default is `4`).
#' @param bt Numeric threshold parameter that influences certain similarity calculations (default is `0.1`).
#'
#' @return A data frame containing matched records from `df1` and `df2`,
#'   with column names suffixed as `.x` (from `df1`) and `.y` (from `df2`).
#'   If `distance_col` is provided, the computed distance values will be included.
#'
#' @examples
#' df1 <- data.frame(Name = c("Alice", "Bob", "Charlie"))
#' df2 <- data.frame(Name = c("Alicia", "Robert", "Charles"))
#' result <- fozzie_join(df1, df2, by = list(Name = "Name"), method = "levenshtein", max_distance = 2, how = "inner", distance_col = "dist")
#' print(result)
#'
#' @export
fozzie_join <- function(df1, df2, by, method='levenshtein', how='inner', max_distance=1, distance_col=NULL, q=NULL, p = 4, bt = 0.1) {
  # Automatically convert character vector `by = c("COL1", "COL2")` into a named list
  if (is.character(by) && length(by) == 2) {
    by <- setNames(list(by[2]), by[1])
  }

  fozzie_join_rs(
		df1=df1,
		df2=df2,
		by=by,
		method=method,
		how=how,
		max_distance=max_distance,
		distance_col=distance_col,
		q=q, p=p, bt=bt)
}

