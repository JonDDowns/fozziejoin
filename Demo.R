library(dplyr)
library(fuzzyjoin)
data(misspellings)
devtools::load_all()

# use the dictionary of words from the qdapDictionaries package,
# which is based on the Nettalk corpus.
library(qdapDictionaries)
words <- tibble::as_tibble(DICTIONARY)

timing_result <- microbenchmark::microbenchmark(
	fuzzyjoin = joined <- misspellings %>%
		stringdist_inner_join(
			words, by = c(misspelling = "word"), max_dist = 1, method='lv'
		),
	fozziejoin = fozzie <- fozzie_join(
		misspellings, words, by = c('misspelling', 'word'), max_distance=1
	),
	times=5
)
print(timing_result)
boxplot(timing_result)

comp_cols <- c(
	'misspelling' = 'misspelling.x',
	'correct' = 'correct.x',
	'word' = 'word.y',
	'syllables' = 'syllables.y'
)
print(dplyr::anti_join(joined, fozzie, by=comp_cols))
