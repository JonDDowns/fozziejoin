library(dplyr)
library(tidyr)
library(ggplot2)
library(microbenchmark)
library(fuzzyjoin)

devtools::install()
library(fozziejoin)

refresh <- FALSE

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

run_bench <- function(method, mode, max_dist, q=NA, nsamp, seed=2016) {

      data(misspellings)

      library(qdapDictionaries)
      words <- tibble::as_tibble(DICTIONARY)

      # Get reproducible sample of records
      set.seed(seed)
      sub_misspellings <- misspellings %>% sample_n(nsamp)

      # Run our comparison function
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
                  fozzie_join(
                        words,
                        by = list('misspelling' = 'word'),
                        method = method,
                        how = mode,
                        max_distance = as.numeric(max_dist),
                        q = q
                  ),
            times = 2
      )
      timing_results <- data.frame(timing_results)
      timing_results$method <- method
      timing_results$time_ms <- timing_results$time / 1e6
      timing_results$n <- round((nrow(sub_misspellings) * nrow(words) / 2) / 1e6)
      
      return(timing_results)
}


bench_file <- file.path("outputs/last_bench.RDS")
if(!file.exists(bench_file) || refresh) {
      results <- lapply(params, function(args, data) {
            cat(paste0("Function params:\n", paste0(args, collapse=", "), "\n"))
            out <- data.frame()
            samp_sizes <- c(100, 1000, 2000)
            for(i in samp_sizes) {
                  cat(paste0("Sampling ", i, " records.\n"))
                  args$nsamp <- i
                  tmp <- do.call(run_bench, args)
                  out <- bind_rows(out, tmp)
            }
            out
      }, data=misspellings)
      results <- do.call(rbind, results)
      saveRDS(results, bench_file)
} else {
      results <- readRDS(bench_file)
}

os <- Sys.info()['sysname']
img_file <- file.path(sprintf("outputs/benchmark_plot_%s.svg", os))
chart_title <- sprintf("Benchmark times of fuzzyjoin vs. fozziejoin inner join methods (%s)", os)
svg(img_file, width = 12, height = 6)
ggplot(results, aes(x=n, y = time_ms, fill = expr, color = expr)) +
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
