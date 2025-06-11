# fozziejoin: Performant data frame joins with inexact matching

[NOTE]: This project is in very early development. It currently depends on the Rust toolchain. APIs may change in the future.

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

Code has been written on a combination of Windows (R 4.3.2, x86_64-w64-mingw32/64) and Linux (R 4.5.0, x86-64-pc-linux-gnu platform).
All builds to date are done using Rust 1.65. 

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
Note that this requires the Rust toolchain and `cargo` to run properly.

```{R}
devtools::install()
```

Alternatively, use `devtools::load_all()` to load the package in development mode.

### Run benchmarks vs. `fuzzyjoin`

Code herein is adapted from the motivating example used in the `fuzzyjoin` package.
First, we take a list of common misspellings (and their corrected alternatives) from Wikipedia.
To run in a a reasonable amount of time, we take a random sample of 1000.

```{r}
library(dplyr)
library(fuzzyjoin)
library(fozziejoin)

# Load misspelling data
data(misspellings)

# Take subset of 1k records
set.seed(2016)
sub_misspellings <- misspellings %>%
  sample_n(1000)
```

Next, we load a dictionary of words from the `qdapDictionaries` package.

```{r}
# Use the dictionary of words from the qdapDictionaries package,
# which is based on the Nettalk corpus.
library(qdapDictionaries)
words <- tibble::as_tibble(DICTIONARY)
```

Then, we use microbenchmark to compare the average clock time of each respective function.

```{r}
# Run each function multiple times and compare results
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

## Known behvavior changes to `fuzzyjoin`

- Matching on columns with `NA` values would throw an error in `fuzzyjoin` but simply do not match in `fozziejoin`. This allows for NA values to persist in left, right, and full joins without matching all NA values to one another.
- The boost threshold (`bt`) and prefix (`p`) parameters are not yet functional for Jaro-Winkler. Currently, all Jaro-Winkler calculations will use default values of 0.1 and 4, respectively. Note the `jw` method defaults in the `stringdist` (and, subsequently, `fuzzyjoin`) package effectively turn these parameters off. See the `jaro` method to replicate this behavior.
- `fozziejoin` always assigns the suffix ".x" to columns from the LHS and ".y" to columns from the RHS. `fuzzyjoin` only does this when both LHS and RHS contain the same column name. `fozziejoin` may conform to the `fuzzyjoin` behavior in the future.

## Acknowledgements

- The `textdistance` crate for most string distance implementations. Currently, this crate is still used for some string distances. Others are based on `textdistance` implementations but with some performance tweaking for this use case.
- The `fuzzyjoin` and `stringdist` packages in R. Much of the project is meant to replicate their APIs and special cases handling.
- The `extendr` team. This project would not be possible without their great project.

## TODO

- [ ] Join Types
    - [X] Inner join
    - [X] Left join
    - [X] Right join
    - [ ] Full join
    - [ ] Anti join
- [ ] Distance Calculations
    - [X] Levenshtein
    - [X] Damerau-Levenshtein
    - [X] Hamming
    - [ ] Longest common substring distance (LCS, current implementation incorrect and not user-accessible)
    - [X] qgram
    - [X] cosine
    - [X] Jaccard
    - [ ] Jaro-Winkler [partial: need to add toggles for p and bt]
    - [X] Jaro
    - [X] OSA
- [ ] Quality of life
    - [ ] Allow for multi-column joins
    - [X] Attach string distance output as column (similar to `distance_col` param in `fuzzyjoin`)
    - [ ] Ignore case for strings
    - [ ] Add parameter to toggle number of threads
- [ ] Install from binary for Windows?
- [ ] Benchmark all methods vs `fuzzyjoin`
- [ ] Proper attribution for all dependencies
- [ ] CRAN distribution

