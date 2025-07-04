#' Perform a fuzzy join between two data frames using approximate string matching.
#'
#' `fozzie_join()` and its directional variants (`fozzie_inner_join()`, `fozzie_left_join()`, `fozzie_right_join()`, `fozzie_anti_join()`, `fozzie_full_join()`)
#' enable approximate matching of string fields in two data frames. These joins support multiple string distance
#' and similarity algorithms including Levenshtein, Jaro-Winkler, q-gram similarity, and others.
#'
#' @param df1 A data frame to join from (left table).
#' @param df2 A data frame to join to (right table).
#' @param by A named list or character vector indicating the matching columns. Can be a character vector of length 2, e.g. `c("col1", "col2")`,
#'   or a named list like `list(col1 = "col2")`.
#' @param method A string indicating the fuzzy matching method. Supported methods:
#'   - `"levenshtein"`: Levenshtein edit distance (default).
#'   - `"osa"`: Optimal string alignment.
#'   - `"damerau_levensthein"` or `"dl"`: Damerau-Levenshtein distance.
#'   - `"hamming"`: Hamming distance (equal-length strings only).
#'   - `"lcs"`: Longest common subsequence.
#'   - `"qgram"`: Q-gram similarity (requires `q`).
#'   - `"cosine"`: Cosine similarity (requires `q`).
#'   - `"jaccard"`: Jaccard similarity (requires `q`).
#'   - `"jaro"`: Jaro similarity.
#'   - `"jaro_winkler"` or `"jw"`: Jaro-Winkler similarity.
#' @param how A string specifying the join mode. One of:
#'   - `"inner"`: matched pairs only.
#'   - `"left"`: all rows from `df1`, unmatched rows filled with NAs.
#'   - `"right"`: all rows from `df2`, unmatched rows filled with NAs.
#'   - `"full"`: all rows from both `df1` and `df2`.
#'   - `"anti"`: rows from `df1` not matched in `df2`.
#' @param q Integer. Size of q-grams for `"qgram"`, `"cosine"`, or `"jaccard"` methods.
#' @param max_distance A numeric threshold for allowable string distance or dissimilarity (lower is stricter).
#' @param distance_col Optional name of column to store computed string distances.
#' @param max_prefix Integer (for Jaro-Winkler) specifying the prefix length influencing similarity boost.
#' @param prefix_weight Numeric (for Jaro-Winkler) specifying the prefix weighting factor.
#' @param nthread Optional integer to specify number of threads for parallelization.
#'
#' @return A data frame with fuzzy-matched rows depending on the join type. See individual functions like `fozzie_inner_join()` for examples.
#'   If `distance_col` is specified, an additional numeric column is included.
#'
#' @examples
#' df1 <- data.frame(name = c("Alice", "Bob", "Charlie"))
#' df2 <- data.frame(name = c("Alicia", "Robert", "Charles"))
#'
#' fozzie_inner_join(df1, df2, by = c("name", "name"), method = "levenshtein", max_distance = 2)
#' fozzie_left_join(df1, df2, by = c("name", "name"), method = "jw", max_distance = 0.2)
#' fozzie_right_join(df1, df2, by = c("name", "name"), method = "cosine", q = 2, max_distance = 0.1)
#'
#' @name fozzie_join_family
#' @export
fozzie_join <- function(df1, df2, by, method = "levenshtein", how = "inner", max_distance = 1,
			distance_col = NULL, q = NULL, max_prefix = 0, prefix_weight = 0, nthread = NULL) {

	# If char vec provided, convert to list.
	if (is.character(by) && length(by) == 2) {
		by <- setNames(list(by[2]), by[1])
	}

	# Run Rust function and return
	fozzie_join_rs(
		df1, df2, by, method, how,
		max_distance, distance_col, q, max_prefix, prefix_weight, nthread
	)
}

#' @rdname fozzie_join_family
#' @return See [fozzie_join()]
#' @export
fozzie_inner_join <- function(df1, df2, by, method = "levenshtein", max_distance = 1,
			      distance_col = NULL, q = NULL, max_prefix = 0, prefix_weight = 0, nthread = NULL) {
	fozzie_join(df1, df2, by, method, max_distance,
		distance_col, q, max_prefix, prefix_weight, nthread, how = "inner")
}

#' @rdname fozzie_join_family
#' @return See [fozzie_join()]
#' @export
fozzie_left_join <- function(df1, df2, by, method = "levenshtein", max_distance = 1,
			     distance_col = NULL, q = NULL, max_prefix = 0, prefix_weight = 0, nthread = NULL) {
	fozzie_join(df1, df2, by, method, max_distance,
		distance_col, q, max_prefix, prefix_weight, nthread, how = "left")
}

#' @rdname fozzie_join_family
#' @return See [fozzie_join()]
#' @export
fozzie_right_join <- function(df1, df2, by, method = "levenshtein", max_distance = 1,
			      distance_col = NULL, q = NULL, max_prefix = 0, prefix_weight = 0, nthread = NULL) {
	fozzie_join(df1, df2, by, method, max_distance,
		distance_col, q, max_prefix, prefix_weight, nthread, how = "right")
}

#' @rdname fozzie_join_family
#' @return See [fozzie_join()]
#' @export
fozzie_anti_join <- function(df1, df2, by, method = "levenshtein", max_distance = 1,
			     distance_col = NULL, q = NULL, max_prefix = 0, prefix_weight = 0, nthread = NULL) {
	fozzie_join(df1, df2, by, method, max_distance,
		distance_col, q, max_prefix, prefix_weight, nthread, how = "anti")
}

#' @rdname fozzie_join_family
#' @return See [fozzie_join()]
#' @export
fozzie_full_join <- function(df1, df2, by, method = "levenshtein", max_distance = 1,
			     distance_col = NULL, q = NULL, max_prefix = 0, prefix_weight = 0, nthread = NULL) {
	fozzie_join(df1, df2, by, method, max_distance,
		distance_col, q, max_prefix, prefix_weight, nthread, how = "full")
}
