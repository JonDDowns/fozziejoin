library(microbenchmark)
library(fozziejoin)
library(fuzzyjoin)
library(tibble)

sizes <- c(2000, 5000, 10000)
seed <- 1337

results <- tibble()

for (size in sizes) {
  cat(sprintf("\nRunning with size %d (%.2f million comparisons)\n", size, round(size^2 / 1e6, 2)))
  set.seed(seed)

  starts1 <- as.integer(round(runif(size, min = 0, max = 500)))
  ends1 <- as.integer(starts1 + round(runif(size, min = 0, max = 10)))
  df1 <- tibble(start = starts1, end = ends1)

  starts2 <- as.integer(round(runif(size, min = 0, max = 500)))
  ends2 <- as.integer(starts2 + round(runif(size, min = 0, max = 10)))
  df2 <- tibble(start = starts2, end = ends2)

  bench <- microbenchmark(
    fuzzy = fuzzy <- interval_join(
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
    times = 15
  )

  if (!identical(as.data.frame(fuzzy), as.data.frame(fozzie))) {
    message("Mismatch detected at size: ", size)
  }

  bench <- as_tibble(bench)
  bench$method <- "interval"
  bench$n_comps <- size ^ 2
  bench$os <- Sys.info()["sysname"]

  results <- rbind(results, bench)
}

# Aggregate results: average and median time by method + n_comps
summary_stats <- aggregate(
  time ~ expr + method + n_comps,
  data = results,
  FUN = function(x) mean(x)
)

# Convert matrix columns to separate columns
summary_df <- tibble(
  expr = summary_stats$expr,
  method = summary_stats$method,
  n_comps = summary_stats$n_comps,
  mean_time = summary_stats$time / 1e6,
)

# Reshape to wide format for ratio calculation
wide_df <- reshape(
  as.data.frame(summary_df),
  idvar = "n_comps",
  timevar = "expr",
  direction = "wide"
)

# Add ratio columns: fuzzy / fozzie
wide_df$mean_ratio <- wide_df$mean_time.fuzzy / wide_df$mean_time.fozzie

# Select and reorder columns for clean output
clean_df <- tibble(
  n_comps = wide_df$n_comps,
  mean_time_fuzzy = wide_df$mean_time.fuzzy,
  mean_time_fozzie = wide_df$mean_time.fozzie,
  mean_ratio = wide_df$mean_ratio,
)

cat("\n⏱️ Timing summary with ratios (fuzzy / fozzie):\n")
print(clean_df)

write.csv(results, "outputs/latest_interval_benchmark.csv", row.names = FALSE)
q("no")

