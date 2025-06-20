library(dplyr)
library(tidyr)
library(ggplot2)
library(microbenchmark)
library(fuzzyjoin)
devtools::load_all()

refresh <- TRUE

params <- list(
	list(method = "osa", mode = "inner", max_dist = 1, q = 0),
	#list(method = "lv", mode = "inner", max_dist = 1, q = 0),
	list(method = "dl", mode = "inner", max_dist = 1, q = 0)
	#list(method = "hamming", mode = "inner", max_dist = 1, q = 0),
	#list(method = "lcs", mode = "inner", max_dist = 1, q = 0), 
	#list(method = "qgram", mode = "inner", max_dist = 2, q = 2)#,
	#list(method = "cosine", mode = "inner", max_dist = 0.9, q = 2)#,
	#list(method = "jaccard", mode = "inner", max_dist = 0.9, q = 2),
	#list(method = "jw", mode = "inner", max_dist = 0.9, q = 0)
)


seed <- 2016
nsamp <- 100
max_dist <- 1
method <- 'lv'

data(misspellings)

library(qdapDictionaries)
words <- tibble::as_tibble(DICTIONARY)

# Get reproducible sample of records
set.seed(seed)
sub_misspellings <- misspellings %>% sample_n(nsamp)

fozzie <- sub_misspellings %>%
	fozzie_join(
		words,
		by = list('misspelling' = 'word'),
		method = 'qgram',
		how = 'left',
		max_distance = as.numeric(max_dist),
		q = 1,
		nthread=1
	)
