# Uncomment to re-install during development
library(dplyr)
library(tidyr)
library(ggplot2)
library(microbenchmark)
library(fuzzyjoin)
library(fozziejoin)
library(qdapDictionaries)

# Load data
data(misspellings)

# create tibble for words
words <- tibble::as_tibble(DICTIONARY)

# Set seed for reproducibility, sample the specified number of recs
set.seed(32)
sub_misspellings <- misspellings %>% sample_n(1000)

# Run benchmark
method <- 'lv'
mode <- 'inner'


fozzie = fozzie <-   fozzie_string_join(
    sub_misspellings,
    words,
    by = list("misspelling" = "word"),
    method = method,
    how = mode,
    max_distance = 1,
)
