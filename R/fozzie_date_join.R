#' @export
fozzie_date_join <- function(
    df1, df2,
    by = NULL,
    how = "inner",
    max_distance = 1,
    distance_col = NULL,
    nthread = NULL) {
  by <- normalize_by(df1, df2, by)

  # Validate that all join columns are Date class
  for (key in names(by)) {
    col1 <- df1[[key]]
    col2 <- df2[[by[[key]]]]

    if (!inherits(col1, "Date") || !inherits(col2, "Date")) {
      stop(sprintf("Column '%s' must be of class 'Date' in both data frames.", key))
    }
  }

  # Call core difference join (no conversion needed)
  tmp <- fozzie_difference_join_rs(
    df1, df2, by,
    how = how,
    max_distance = max_distance,
    distance_col = distance_col,
    nthread = nthread
  )

  data.frame(tmp)
}
