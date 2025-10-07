library(microbenchmark)
library(fozziejoin)
library(fuzzyjoin)
library(data.table)
set.seed(1337)

sizes <- c(3000, 5000, 7000, 9000)

for (size in sizes) {
  cat("\nMillions of comparisons:", round(size^2 / 1e6, 2), "\n")

  # Generate interval data
  starts1 <- as.integer(round(runif(size, min = 0, max = 500)))
  ends1 <- as.integer(starts1 + round(runif(size, min = 0, max = 10)))
  df1 <- data.frame(start = starts1, end = ends1)

  starts2 <- as.integer(round(runif(size, min = 0, max = 500)))
  ends2 <- as.integer(starts2 + round(runif(size, min = 0, max = 10)))
  df2 <- data.frame(start = starts2, end = ends2)

  # Run benchmark
  timing_results <- microbenchmark(
    fuzzyjoin = fuzzy <- interval_join(
      df1, df2,
      by = c("start", "end"),
      mode = "inner",
      maxgap = 0,
      minoverlap = 0
    ),
    fozzie = fozzie <- fozzie_interval_join(
      df1, df2,
      by = list(start = "start", end = "end"),
      how = "inner",
      overlap_type = "any",
      maxgap = 0,
      minoverlap = 0,
      interval_mode = "integer"
    ),
    times = 10
  )

  # Align column names for comparison
  fuzzy <- data.frame(fuzzy)

  # Confirm all results are the same
  if (!identical(fozzie, fuzzy)) {
    only_fozzie <- dplyr::anti_join(fozzie, fuzzy, by = colnames(fozzie))
    if (nrow(only_fozzie) > 0) {
      print(head(only_fozzie))
    }

    only_fuzzy <- dplyr::anti_join(fuzzy, fozzie, by = colnames(fozzie))
    if (nrow(only_fuzzy) > 0) {
      print(head(only_fuzzy))
    }
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

  cat("⏱️ Timing results:\n")
  print(timing_summary)
}
