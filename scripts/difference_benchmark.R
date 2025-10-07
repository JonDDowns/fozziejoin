library(microbenchmark)
library(fozziejoin)
library(fuzzyjoin)
library(dplyr)
set.seed(42)

sizes <- c(5000, 10000, 20000, 31623)

for (size in sizes) {
  print(paste("Millions of comparisons:", round(size^2 / 1e6, 2)))
  df1 <- tibble::tibble(
    x = runif(size, min = 0, max = 500)
  )
  df2 <- tibble::tibble(
    x = runif(size, min = 0, max = 500)
  )
  timing_results <- microbenchmark(
    fuzzy = fuzzy <- difference_join(
      df1, df2,
      mode = "semi", max_dist = 1, by = c("x")
    ),
    fozzie = fozzie <- fozzie_difference_join(
      df1, df2,
      how = "semi", max_distance = 1, by = c("x")
    ),
    times = 10
  )

  # Confirm all results are the same
  if (!isTRUE(all.equal(fuzzy, fozzie))) {
    print("Not all observations equal! Differences:")
    diffs <- anti_join(fozzie, fuzzy, by = c("x.x", "x.y"))
    if (nrow(diffs) > 0) {
      print("Max difference for fozzie mismatches:")
      max_dif <- max(abs(diffs$x.x - diffs$x.y))
      print(max_dif)
    }
    diffs <- anti_join(fuzzy, fozzie)
    if (nrow(diffs) > 0) {
      print("Max difference for fuzzy mismatches:")
      max_dif <- max(abs(diffs$x.x - diffs$x.y))
      print(max_dif)
    }
  }

  # Customize output
  timing_results <- data.frame(timing_results)
  timing_results$time_ms <- timing_results$time / 1e6
  timing_results$mill_comps <- round((nrow(df1) * nrow(df2)) / 1e6, 1)
  timing_results$os <- Sys.info()["sysname"]

  # Get mean run time by group
  timing_summary <- aggregate(time_ms ~ expr + mill_comps, data = timing_results, FUN = mean)
  timing_summary <- timing_summary[order(timing_summary$expr), ]
  timing_summary$ratio <- timing_summary$time_ms / timing_summary$time_ms[[2]]

  print("Timing results:")
  print(timing_summary)
}
