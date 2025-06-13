library(dplyr)
library(tidyr)
library(ggplot2)
library(microbenchmark)
library(fuzzyjoin)
library(fozziejoin)

params <- tibble::tibble(
	method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw"),
	mode = "inner",
	max_dist = c(1, 1, 1, 1, 1, 2, 0.9, 0.9, 0.9),
	q = c(0, 0, 0, 0, 0, 2, 2, 2, 0)
)

data(misspellings)

library(qdapDictionaries)
words <- tibble::as_tibble(DICTIONARY)

# Collect timing results
all <- data.frame()
for (i in c(100, 1000, 2000)) {
	# Get reproducible sample of records
	set.seed(2016)
	sub_misspellings <- misspellings %>% sample_n(i)

	# Run our comparison function
	timing_results <- params %>%
		rowwise() %>%
		mutate(
			benchmark = list(microbenchmark(
				fuzzy = sub_misspellings %>%
					stringdist_join(
						words,
						by = c(misspelling = "word"),
						method = method,
						mode = mode,
						max_dist = as.numeric(max_dist),
						q = as.numeric(q)
					),
				fozzie = sub_misspellings %>%
					fozzie_join(
						words,
						by = list('misspelling' = 'word'),
						method = method,
						how = mode,
						max_distance = as.numeric(max_dist),
						q = as.numeric(q)
					),
				times = 3
			))
		) %>%
		unnest(benchmark) %>%
		select(method, expr, time)

	if (!all.equal(fozzie, data.frame(fuzzy))) {
		stop("Results are not the same")
	}

	timing_results <- timing_results %>%
		mutate(time_ms = time / 1e6, n = i)

	all <- bind_rows(all, timing_results)
}

# Plot with ggplot2
ggplot(all, aes(x=n, y = time_ms, fill = expr, color = expr)) +
	geom_point() +
	facet_wrap(~ method, scales='free') +
	labs(title = "Benchmarking String Distance Methods",
		x = "Method", y = "Execution Time (ms)") +
	theme_minimal() +
	geom_smooth(method='lm', se=F)

