library(microbenchmark)
library(fuzzyjoin)
library(tibble)
library(dplyr)
library(fozziejoin)

# Create large input data
set.seed(1337)
n <- 5e6
vals <- c(
    "apple", "banana", "cherry", "melon", "grape", "kiwi", "peach", "plum",
    "pear", "fig", "yurt", "tomato"
)
df1 <- tibble(name = sample(vals, n, replace = TRUE))

# Generate regex patterns targeting different fruit substrings and anchors
base_patterns <- c(
    "^a", "an", "rry$", "melon", "\bgrape\b", "kiwi", "pea", "pl", "ch", "fig"
)
df2 <- tibble(pattern = sample(base_patterns, 100, replace = TRUE))

# Define benchmark functions
bench_fozzie <- function() {
  fozzie_regex_inner_join(
        df1, df2, by = c("name" = "pattern"), ignore_case = FALSE, nthread = 1
  )
}

bench_fuzzyjoin <- function() {
  regex_inner_join(df1, df2, by = c("name" = "pattern"), ignore_case = FALSE)
}

# Run benchmark
microbenchmark(
  fozzie = fozzie <- bench_fozzie(),
  fuzzyjoin = fuzzy <- bench_fuzzyjoin(),
  times = 10,
  unit = "ms"
)
fozzie <- fozzie %>% arrange(name, pattern)
fuzzy <- fuzzy %>% arrange(name, pattern)
print(identical(fozzie, fuzzy))

