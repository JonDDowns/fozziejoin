library(microbenchmark)
library(fozziejoin)
library(fuzzyjoin)
library(dplyr)
set.seed(42)

sizes <- c(5000, 10000, 20000)

for (size in sizes) {
  print(paste("Millions of comparisons:", round(size^2 / 1e6, 2)))

  # Create 2–3 numeric columns
  df1 <- data.frame(
    x = round(runif(size, min = 0, max = 100), 2),
    y = round(runif(size, min = 0, max = 100), 2)
  )
  df2 <- data.frame(
    x = round(runif(size, min = 0, max = 100), 2),
    y = round(runif(size, min = 0, max = 100), 2)
  )

  # Columns to match on
  match_cols <- c("x", "y")

  # Run benchmark
  timing_results <- microbenchmark(
    fuzzy = fuzzy <- distance_join(
      df1, df2,
      method = "manhattan",
      mode = "inner",
      max_dist = 1,
      by = match_cols,
      distance_col = "dist"
    ),
    fozzie = fozzie <- fozzie_distance_join(
      df1, df2,
      method = "manhattan",
      how = "inner",
      max_distance = 1,
      by = match_cols,
      distance_col = "dist"
    ),
    times = 10
  )

  # Align column names for comparison
  colnames(fuzzy) <- colnames(fozzie)
  fuzzy <- data.frame(fuzzy)

  # Confirm all results are the same
  if (!isTRUE(all.equal(fuzzy, fozzie))) {
    print("Not all observations equal!")
  }

  # Format timing output
  timing_results <- data.frame(timing_results)
  timing_results$time_ms <- timing_results$time / 1e6
  timing_results$mill_comps <- round((nrow(df1) * nrow(df2)) / 1e6, 1)
  timing_results$os <- Sys.info()["sysname"]

  # Summary
  timing_summary <- aggregate(time_ms ~ expr + mill_comps, data = timing_results, FUN = mean)
  timing_summary <- timing_summary[order(timing_summary$expr), ]
  timing_summary$ratio <- timing_summary$time_ms / timing_summary$time_ms[[2]]

  print("Timing results:")
  print(timing_summary)
}
