# Uncomment to re-install during development
library(dplyr)
library(tidyr)
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
        list(method = "jw", mode = "inner", max_dist = 0.9, q = 0)
)

# If running in script mode, use user input to set methods to call
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  params <- Filter(function(p) p$method %in% args, params)
}

# Compares runtimes for fuzzyjoin and fozziejoin for a given set of parameters
run_bench <- function(method, mode, max_dist, q=NA, nsamp, seed=2016) {
  # Load data
  data(misspellings)

  # create tibble for words
  words <- tibble::as_tibble(DICTIONARY)

  # Set seed for reproducibility, sample the specified number of recs
  set.seed(seed)
  sub_misspellings <- misspellings %>% sample_n(nsamp)

  # Run benchmark
  timing_results <- microbenchmark(
    fuzzy = fuzzy <- stringdist_join(
      sub_misspellings,
      words,
      by = c(misspelling = "word"),
      method = method,
      mode = mode,
      max_dist = as.numeric(max_dist),
      q = q
    ),
    fozzie = fozzie <- sub_misspellings %>%
      fozzie_string_join(
        words,
        by = list('misspelling' = 'word'),
        method = method,
        how = mode,
        max_distance = as.numeric(max_dist),
        q = q
      ),
    times = 2
  )

  # Get fuzzy df in same format as fozzie to do a direct comparison
  colnames(fuzzy) <- colnames(fozzie)
  fuzzy <- data.frame(fuzzy)

  # Confirm all results are the same
  if(!isTRUE(all.equal(fuzzy, fozzie))) {
    print("Not all observations equal! differences")
    print(all.equal(fuzzy, fozzie))
  }

  # Customize output
  timing_results <- data.frame(timing_results)
  timing_results$method <- method
  timing_results$time_ms <- timing_results$time / 1e6
  timing_results$mill_comps <- round((nrow(sub_misspellings) * nrow(words)) / 1e6, 1)
  timing_results$os <- Sys.info()['sysname']

  # Get mean run time by group
  timing_summary <- aggregate(time_ms ~ expr + mill_comps + method, data=timing_results, FUN=mean)
  timing_summary <- timing_summary[order(timing_summary$expr), ]
  timing_summary$ratio <- timing_summary$time_ms / timing_summary$time_ms[[2]]

  print("Timing results:")
  print(timing_summary)

  # Return
  return(timing_results)
}


# Run the function for all desired benchmarks and save the result to file.
tnow <- format(Sys.time(), "%Y%m%d_%H%M%S")
bench_file <- file.path(sprintf("outputs/last_bench_%s.RDS", tnow))
results <- lapply(
  params,
  function(args, data) {
    cat(paste0("Function params:\n", paste0(args, collapse=", "), "\n"))
    out <- data.frame()
    samp_sizes <- c(100, 500, 1000, 2000, 3000)
    for(i in samp_sizes) {
      cat(paste0("Sampling ", i, " records.\n"))
      args$nsamp <- i
      tmp <- do.call(run_bench, args)
      out <- bind_rows(out, tmp)
    }
    out
  },
  data=misspellings
)
results <- do.call(rbind, results)
saveRDS(results, bench_file)

# Determine operating system, set chart title and plot name
os <- Sys.info()['sysname']
img_file <- file.path(sprintf("outputs/string_inner_bench_latest.svg", os, tnow))
chart_title <- sprintf("Benchmark times of fuzzyjoin vs. fozziejoin inner join methods (%s)", os)

# Generate plot
svg(img_file, width = 12, height = 6)
ggplot(results, aes(x=mill_comps, y = time_ms, fill = expr, color = expr)) +
        geom_point() +
        facet_wrap(~ method, scales='free') +
        labs(
                title = chart_title,
                x = "Number of comparisons (millions)",
                y = "Execution Time (ms)",
    fill = "Package",
    color = "Package"

          ) +
        theme_minimal() +
        geom_smooth(method='lm', se=FALSE) +
        scale_y_continuous(labels = scales::comma) +
        scale_x_continuous(labels = scales::comma)
dev.off()

# Done!
q(status = 0, save = "no")
