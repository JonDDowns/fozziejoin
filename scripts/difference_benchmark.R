library(microbenchmark)
library(fozziejoin)
library(fuzzyjoin)
library(tibble)

sizes <- c(1000, 5000, 10000)
seed <- 1337

results <- data.frame()

for (size in sizes) {
  cat(sprintf("Running with size %d (%.2f million comparisons)\n", size, round(size^2 / 1e6, 2)))
  set.seed(seed)

  df1 <- tibble::tibble(x = runif(size, min = 0, max = 500))
  df2 <- tibble::tibble(x = runif(size, min = 0, max = 500))

  bench <- microbenchmark(
    fuzzy = fuzzy <- difference_join(
      df1, df2,
      mode = "inner", max_dist = 1, by = "x"
    ),
    fozzie = fozzie <- fozzie_difference_join(
      df1, df2,
      how = "inner", max_distance = 1, by = "x"
    ),
    times = 10
  )

  # Order results before comparison
  fuzzy <- fuzzy[do.call(order, as.data.frame(fuzzy)), ]
  fozzie <- fozzie[do.call(order, as.data.frame(fozzie)), ]

  if (!isTRUE(all.equal(fuzzy, fozzie))) {
    message("Mismatch detected at size: ", size)
    print(nrow(fuzzy))
    print(nrow(fozzie))
  }

  bench <- data.frame(bench)
  bench$method <- "difference"
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
summary_df <- data.frame(
  expr = summary_stats$expr,
  method = summary_stats$method,
  n_comps = summary_stats$n_comps,
  mean_time = summary_stats$time / 1e6
)

# Reshape to wide format for ratio calculation
wide_df <- reshape(
  summary_df,
  idvar = "n_comps",
  timevar = "expr",
  direction = "wide"
)

# Add ratio column: fuzzy / fozzie
wide_df$mean_ratio <- wide_df$mean_time.fuzzy / wide_df$mean_time.fozzie

# Select and reorder columns for clean output
clean_df <- tibble(wide_df[, c("n_comps", "mean_time.fuzzy", "mean_time.fozzie",
                        "mean_ratio")])

# Print cleaned summary
cat("\nDifference timing summary with ratios (fuzzy / fozzie):\n")
print(clean_df)

write.csv(results, "outputs/latest_difference_benchmark.csv", row.names = FALSE)
q("no")

