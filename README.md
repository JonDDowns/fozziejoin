# fozziejoin: Performant data frame joins with inexact matching

[NOTE]: This project is in very early development and for demonstration purposes only.
The main achievement to date is proving a performance gain in a single use case versus the `stringdist_inner_join` function in `fuzzyjoin`.

The `fozziejoin` package uses Rust to perform R dataframe joins based on string distance metrics.
It is intended to be a high-performance alternative to `stringdist_inner_join` and similar functions from the [fuzzyjoin package](https://github.com/dgrtwo/fuzzyjoin).

Performance gains come from tailoring string distance calculation to the use case of fuzzy joins in order to eliminate unnecessary steps.
The `fuzzyjoin` package depends on the [`stringdist` package](https://github.com/markvanderloo/stringdist) for string distance-based joins.
While the `stringdist` package is very performant and multithreaded, it is not tailored to this use case.
Thus, `fozziejoin` makes the following changes to improve performance:

- No duplicate string distance calculations are performed. You only need compare "Jon" and "John" once.
- No intermediate R objects, such as a matrix of string distance lengths, are created. All calculations are done in Rust, and data are exchanged between R and Rust using the [`extendr` Rust crate](https://github.com/extendr/extendr).

The name itself is a bit of wordplay: the common term for this task is 'fuzzy join', which is similar to [Fozzie Bear](https://en.wikipedia.org/wiki/Fozzie_Bear) from the Muppets. 
A picture of Fozzie will be in the repo once a stronger legal team is in place.
Wocka wocka!

## Getting started

All code was written and tested on R 4.5.0 (x86-64-pc-linux-gnu platform) and Rust 1.65. 

### Pre-requisites

- R (version 4.5.0 preferred)
- The rust toolchain (`rustup`) and package manager (`cargo`)
- The `rextendr` R package
- The `devtools` R package
- The following R packages are required to run examples:
    - `dplyr`
    - `fuzzyjoin`
    - `qdapDictionaries`
    - `microbenchmark`

### Installation

First, clone the repo:

```{sh}
git clone https://github.com/JonDDowns/fozziejoin
cd ./fozziejoin
```

Then, use `devtools` to install the package.

```{R}
devtools::install()
```

Alternatively, use `devtools::load_all()` to laod the package in development mode.

### Run benchmarks vs. `fuzzyjoin`

Code herein is adapted from the motivating example used in the `fuzzyjoin` package.
First, we take a list of common misspellings (and their corrected alternatives) from Wikipedia.
To run in a a reasonable amount of time, we take a random sample of 1000.

```{r}
library(dplyr)
library(fuzzyjoin)
library(qdapDictionaries)
library(fozziejoin)

# Load misspelling data
data(misspellings)

# Take subset of 1k records
set.seed(2016)
sub_misspellings <- misspellings %>%
  sample_n(1000)
```

Next, we load a dictionary of words from the qdapDictionaries package.

```{r}
# Use the dictionary of words from the qdapDictionaries package,
# which is based on the Nettalk corpus.
library(qdapDictionaries)
words <- tibble::as_tibble(DICTIONARY)
```

Then, we use microbenchmark with 5 runs to compare the average clock time of each respective function.

```{r}
# Run each function 5x and compare results
timing_result <- microbenchmark::microbenchmark(
	fuzzyjoin = joined <- sub_misspellings %>%
		stringdist_inner_join(
			words, by = c(misspelling = "word"), max_dist = 2, method='lv'
		),
	fozziejoin = fozzie <- fozzie_join(
		sub_misspellings, words, by = c('misspelling', 'word'), max_distance=2
	),
	times=5
)
print(timing_result)

#> Unit: milliseconds
#>    expr        min         lq      mean     median        uq       max neval
#>   fuzzy 2151.74391 2165.40659 2348.9678 2268.66249 2369.7904 2789.2356     5
#>  fozzie   96.99412   97.38295   99.3314   99.36834  100.7067  102.2049     5
```

And we confirm the results are the same:

```{r}
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
#> [1] "Number of records in fuzzyjoin but not in fozziejoin: 0"

# Check for fozziejoin records not in fuzzyjoin
# Swap names and values
swapped_cols <- setNames(names(comp_cols), comp_cols)
not_in_fozzie <- dplyr::anti_join(fozzie, joined, by=swapped_cols)
print(paste(
	"Number of records in fozziejoin but not in fuzzyjoin:",
	nrow(not_in_fozzie)
))
#> [1] "Number of records in fozziejoin but not in fuzzyjoin: 0"
```

## TODO

Basically everything.

- [ ] Join Types
    - [X] Inner join
    - [ ] Left join
    - [ ] Right join
    - [ ] Full join
    - [ ] Semi join
    - [ ] Anti join
    - [ ] OSA
- [ ] Distance Calculations
    - [X] Levenshtein
    - [ ] Damerau-Levenshtein
    - [ ] Hamming
    - [ ] Longest common substring distance (LCS).
    - [ ] qgram	
    - [ ] cosine
    - [ ] Jaccard
    - [ ] Jaro-Winkler
- [ ] Quality of life
    - [ ] Allow for multi-column joins
    - [ ] Attach string distance output as a new column
    - [ ] Ignore case
- [ ] CRAN distribution

