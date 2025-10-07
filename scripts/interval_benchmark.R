library(microbenchmark)
#library(fozziejoin)
devtools::load_all()
library(fuzzyjoin)
library(data.table)
set.seed(42)

sizes <- c(1000, 3000, 5000, 7000)

for (size in sizes) {
  cat("\nMillions of comparisons:", round(size^2 / 1e6, 2), "\n")

  # Generate interval data
  starts1 <- as.integer(round(runif(size, min = 0, max = 100)))
  ends1 <- as.integer(starts1 + round(runif(size, min = 0, max = 10)))
  df1 <- data.frame(start = starts1, end = ends1)

  starts2 <- as.integer(round(runif(size, min = 0, max = 100)))
  ends2 <- as.integer(starts2 + round(runif(size, min = 0, max = 10)))
  df2 <- data.frame(start = starts2, end = ends2)

  # Convert to data.table and set keys for foverlaps
  dt1 <- as.data.table(df1)
  dt2 <- as.data.table(df2)
  setnames(dt1, c("start", "end"), c("start1", "end1"))
  setnames(dt2, c("start", "end"), c("start2", "end2"))
  setkey(dt1, start1, end1)
  setkey(dt2, start2, end2)

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
      maxgap = 0L,
      minoverlap = 0L,
      interval_mode = "integer"
    ),
    times = 10
  )
  # Align column names for comparison
  fuzzy <- data.frame(fuzzy)

  # Confirm all results are the same
  if (!isTRUE(all.equal(fozzie, fuzzy))) {
    cat("❌ Not all observations equal!\n")
    print(nrow(fozzie))
    print(nrow(fuzzy))
    only_fozzie <- dplyr::anti_join(fozzie, fuzzy) |> dplyr::arrange()
    print(nrow(only_fozzie))
    print(head(only_fozzie))

    only_fuzzy <- dplyr::anti_join(fuzzy, fozzie) |> dplyr::arrange()
    print(nrow(only_fuzzy))
    print(head(only_fuzzy))
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
