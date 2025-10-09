library(microbenchmark)
library(fozziejoin)
library(fuzzyjoin)
library(tibble)

sizes <- c(1000, 5000, 10000)
seed <- 1337

results <- tibble()

for (size in sizes) {
  cat(sprintf("Running with size %d (%.2f million comparisons)\n", size, round(size^2 / 1e6, 2)))
  set.seed(seed)

  df1 <- tibble(
    x = round(runif(size, min = 0, max = 100), 2),
    y = round(runif(size, min = 0, max = 100), 2)
  )
  df2 <- tibble(
    x = round(runif(size, min = 0, max = 100), 2),
    y = round(runif(size, min = 0, max = 100), 2)
  )

  match_cols <- c("x", "y")

  bench <- microbenchmark(
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

  if (!isTRUE(all.equal(as.data.frame(fuzzy), as.data.frame(fozzie)))) {
    message("Mismatch detected at size: ", size)
    print(nrow(fuzzy))
    print(nrow(fozzie))
  }

  bench <- as_tibble(bench)
  bench$method <- "distance"
  bench$n_comps <- size ^ 2
  bench$os <- Sys.info()["sysname"]

  results <- rbind(results, bench)
}

# Aggregate results: average and median time by method + n_comps
summary_stats <- aggregate(
  time ~ expr + method + n_comps,
  data = results,
  FUN = function(x) mean = mean(x)
)

# Convert matrix columns to separate columns
summary_df <- tibble(
  expr = summary_stats$expr,
  method = summary_stats$method,
  n_comps = summary_stats$n_comps,
  mean_time = summary_stats$time / 1e6
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

cat("\nDistance timing summary with ratios (fuzzy / fozzie):\n")
print(clean_df)

write.csv(results, "outputs/latest_distance_benchmark.csv", row.names = FALSE)
q("no")

