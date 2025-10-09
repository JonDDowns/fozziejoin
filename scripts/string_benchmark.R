library(microbenchmark)
library(fuzzyjoin)
library(fozziejoin)
library(qdapDictionaries)
library(tibble)

samp_sizes <- c(250, 500, 750)
seed <- 2016

params <- list(
  list(method = "osa", mode = "inner", max_dist = 1, q = 0),
  list(method = "lv", mode = "inner", max_dist = 1, q = 0),
  list(method = "dl", mode = "inner", max_dist = 1, q = 0),
  list(method = "hamming", mode = "inner", max_dist = 1, q = 0),
  list(method = "lcs", mode = "inner", max_dist = 1, q = 0),
  list(method = "qgram", mode = "inner", max_dist = 2, q = 2),
  list(method = "cosine", mode = "inner", max_dist = 0.5, q = 2),
  list(method = "jaccard", mode = "inner", max_dist = 0.5, q = 2),
  list(method = "jw", mode = "inner", max_dist = 0.5, q = 0),
  list(method = "soundex", mode = "inner", max_dist = 0.5, q = 0)
)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  params <- Filter(function(p) p$method %in% args, params)
}

data(misspellings)
words <- as.data.frame(DICTIONARY)

results <- data.frame()

for (p in params) {
  for (nsamp in samp_sizes) {
    cat(sprintf("Running %s with %d samples\n", p$method, nsamp))
    set.seed(seed)
    sub_misspellings <- misspellings[sample(seq_len(nrow(misspellings)), nsamp), ]

    bench <- microbenchmark(
      fuzzy = fuzzy <- stringdist_join(
        sub_misspellings, words,
        by = c(misspelling = "word"),
        method = p$method, mode = p$mode,
        max_dist = p$max_dist, q = p$q
      ),
      fozzie = fozzie <- fozzie_string_join(
        sub_misspellings, words,
        by = c(misspelling = "word"),
        method = p$method, how = p$mode,
        max_distance = p$max_dist, q = p$q
      ),
      times = 10
    )

    if (!isTRUE(all.equal(fuzzy, fozzie)) && p$method != "soundex") {
      message("Mismatch detected for method: ", p$method)
    }

    df <- as.data.frame(bench)
    df$method <- p$method
    df$n_comps <- nrow(sub_misspellings) * nrow(words)
    df$os <- Sys.info()["sysname"]

    results <- rbind(results, df)
  }
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
  idvar = c("n_comps", "method"),
  timevar = "expr",
  direction = "wide"
)

# Add ratio column: fuzzy / fozzie
wide_df$mean_ratio <- wide_df$mean_time.fuzzy / wide_df$mean_time.fozzie

# Select and reorder columns for clean output
keep_cols <- c(
    "method",
    "n_comps",
    "mean_time.fuzzy",
    "mean_time.fozzie", 
    "mean_ratio"
)
clean_df <- tibble(wide_df[order(wide_df$method), keep_cols])

# Print cleaned summary
cat("\nTiming summary with ratios (fuzzy / fozzie):\n")
print(clean_df)

write.csv(results, "outputs/latest_string_benchmark.csv", row.names = FALSE)
q("no")

