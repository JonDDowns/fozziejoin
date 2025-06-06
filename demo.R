library(dplyr)
library(fuzzyjoin)
library(fozziejoin)

# Load misspelling data
data(misspellings)

set.seed(2016)
sub_misspellings <- misspellings %>%
	sample_n(100)

method <- 'lv'
max_dist <- 2
q <- 3

# Use the dictionary of words from the qdapDictionaries package,
# which is based on the Nettalk corpus.
library(qdapDictionaries)
words <- tibble::as_tibble(DICTIONARY)

# Run each function 5x and compare results
timing_result <- microbenchmark::microbenchmark(
	fuzzy = joined <- sub_misspellings %>%
		stringdist_inner_join(
			words,
			by = c(misspelling = "word"),
			max_dist=max_dist,
			method=method,
	),
	fozzie = fozzie <- fozzie_join(
		sub_misspellings,
		words,
		by = list('misspelling' = 'word'),
		max_distance=max_dist,
		method = method,
		how="inner",
	),
	times=5
)
print(timing_result)

# Prove dataframe contents are the same
# NOTE: outputs would not currently pass the `identical` check

# Check for fuzzyjoin records not in fozziejoin
comp_cols <- c(
	'misspelling' = 'misspelling.x',
	'correct' = 'correct.x',
	'word' = 'word.y',
	'syllables' = 'syllables.y'
)
only_in_fuzzy <- dplyr::anti_join(joined, fozzie, by=comp_cols, na_matches="na")
print(paste(
	"Number of records in fuzzyjoin but not in fozziejoin:",
	nrow(only_in_fuzzy)
))
print(only_in_fuzzy)

# Check for fozziejoin records not in fuzzyjoin
# Swap names and values
swapped_cols <- setNames(names(comp_cols), comp_cols)
only_in_fozzie <- dplyr::anti_join(as_tibble(fozzie), joined, by=swapped_cols)
print(paste(
	"Number of records in fuzzyjoin but not in fozziejoin:",
	nrow(only_in_fozzie)
))


