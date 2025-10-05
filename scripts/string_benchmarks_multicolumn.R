# Load required libraries
library(ggplot2)
library(microbenchmark)
library(fuzzyjoin)
library(fozziejoin)
library(qdapDictionaries)

# These are all the benchmarks we may wish to run
params <- list(
  list(method = "osa", mode = "inner", max_dist = 1, q = 0),
  list(method = "lv", mode = "inner", max_dist = 1, q = 0),
  list(method = "dl", mode = "inner", max_dist = 1, q = 0),
  list(method = "hamming", mode = "inner", max_dist = 1, q = 0),
  list(method = "lcs", mode = "inner", max_dist = 1, q = 0),
  list(method = "qgram", mode = "inner", max_dist = 2, q = 2),
  list(method = "cosine", mode = "inner", max_dist = 0.9, q = 2),
  list(method = "jaccard", mode = "inner", max_dist = 0.9, q = 2),
  list(method = "jw", mode = "inner", max_dist = 0.9, q = 0),

  # WARNING:
  # Soundex joins are similar but NOT identical
  list(method = "soundex", mode = "inner", max_dist = 0.5, q = 0)
)

# Optional method filtering via command line
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  params <- Filter(function(p) p$method %in% args, params)
}

# Benchmark function
run_bench <- function(method, mode, max_dist, q = NA, nsamp, seed = 2016) {
  data(misspellings)
  words <- as.data.frame(DICTIONARY)
  set.seed(seed)
  sub_misspellings <- misspellings[sample(nrow(misspellings), nsamp), ]

  timing_results <- microbenchmark(
    fuzzy = {
      fuzzy <- stringdist_join(
        sub_misspellings, words,
        by = c(misspelling = "word", correct = "word"),
        method = method, mode = mode,
        max_dist = max_dist, q = q
      )
    },
    fozzie = {
      fozzie <- fozzie_string_join(
        sub_misspellings, words,
        by = list("misspelling" = "word", correct = "word"),
        method = method, how = mode,
        max_distance = max_dist, q = q
      )
    },
    times = 10
  )

  # Compare outputs (soundex not expected to be identical)
  fuzzy <- as.data.frame(fuzzy)
  if (!isTRUE(all.equal(fuzzy, fozzie)) & method != "soundex") {
    cat("Mismatch detected:\n")
    print(all.equal(fuzzy, fozzie))
  }

  # Format timing results
  timing_df <- as.data.frame(timing_results)
  timing_df$method <- method
  timing_df$time_ms <- timing_df$time / 1e6
  timing_df$mill_comps <- round(nrow(sub_misspellings) * nrow(words) / 1e6, 1)
  timing_df$os <- Sys.info()["sysname"]

  # Summary
  exprs <- unique(timing_df$expr)
  summary_df <- do.call(rbind, lapply(exprs, function(e) {
    subset <- timing_df[timing_df$expr == e, ]
    data.frame(
      expr = e,
      mill_comps = subset$mill_comps[1],
      method = method,
      time_ms = mean(subset$time_ms)
    )
  }))
  summary_df <- summary_df[order(summary_df$expr), ]
  summary_df$ratio <- summary_df$time_ms / summary_df$time_ms[summary_df$expr == "fozzie"]

  cat("Timing summary:\n")
  print(summary_df)

  return(timing_df)
}

# Run benchmarks
tnow <- format(Sys.time(), "%Y%m%d_%H%M%S")
bench_file <- sprintf("outputs/last_bench_%s.RDS", tnow)

results <- list()
for (p in params) {
  cat("Running method:", p$method, "\n")
  for (n in c(1000, 2000, 3000)) {
    cat("  Sample size:", n, "\n")
    p$nsamp <- n
    res <- do.call(run_bench, p)
    results <- append(results, list(res))
  }
}
results_df <- do.call(rbind, results)
saveRDS(results_df, bench_file)

# Plot results
os <- Sys.info()["sysname"]
img_file <- sprintf("outputs/bench_string_multicolumn_%s_latest.svg", os)
chart_title <- sprintf("String distance inner join: fuzzyjoin vs fozziejoin, multiple columns (%s)", os)

svg(img_file, width = 12, height = 6)
ggplot(results_df, aes(x = mill_comps, y = time_ms, fill = expr, color = expr)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE) +
  facet_wrap(~method, scales = "free") +
  labs(
    title = chart_title,
    x = "Number of comparisons (millions)",
    y = "Execution Time (ms)",
    fill = "Package",
    color = "Package"
  ) +
  theme_minimal() +
  scale_y_continuous(labels = scales::comma) +
  scale_x_continuous(labels = scales::comma)
dev.off()

# Exit
q(status = 0, save = "no")
