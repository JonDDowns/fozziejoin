library(dplyr)
library(fuzzyjoin)
library(fozziejoin)

# Load misspelling data
data(misspellings)

set.seed(2016)
sub_misspellings <- misspellings %>%
  sample_n(1000)


# Use the dictionary of words from the qdapDictionaries package,
# which is based on the Nettalk corpus.
library(qdapDictionaries)
words <- tibble::as_tibble(DICTIONARY)

# Run each function 5x and compare results
timing_result <- microbenchmark::microbenchmark(
	fuzzy = joined <- sub_misspellings %>%
		stringdist_inner_join(
			words, by = c(misspelling = "word"), max_dist = 2, method='lv'
		),
	fozzie = fozzie <- fozzie_join(
		sub_misspellings, words, by = c('misspelling', 'word'), max_distance=2
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
not_in_fuzzy <- dplyr::anti_join(joined, fozzie, by=comp_cols)
print(paste(
	"Number of records in fuzzyjoin but not in fozziejoin:",
	nrow(not_in_fuzzy)
))

# Check for fozziejoin records not in fuzzyjoin
# Swap names and values
swapped_cols <- setNames(names(comp_cols), comp_cols)
not_in_fozzie <- dplyr::anti_join(fozzie, joined, by=swapped_cols)
print(paste(
	"Number of records in fuzzyjoin but not in fozziejoin:",
	nrow(not_in_fozzie)
))
