# fozziejoin 🧸

> ⚠️ **Note**: This project is in early development.
> APIs may change, and installing from source requires the Rust toolchain.

`fozziejoin` is an R package that uses Rust to perform high-performance dataframe joins based on string distance metrics.
It’s designed as a fast alternative to functions like `stringdist_inner_join()` from the [fuzzyjoin package](https://github.com/dgrtwo/fuzzyjoin).

Unlike the [`stringdist` package](https://github.com/markvanderloo/stringdist), which computes all pairwise distances before filtering, `fozziejoin` implements string distance algorithms optimized for threshold-based filtering.
This avoids unnecessary computation, improves memory efficiency, and speeds up performance in real-world applications.

The name is a playful nod to “fuzzy join” — reminiscent of [Fozzie Bear](https://en.wikipedia.org/wiki/Fozzie_Bear) from the Muppets.
A picture of Fozzie will appear in the repo once the legal team gets braver.
**Wocka wocka!**

## Getting started

Code has been written on a combination of Windows (R 4.3.2, x86_64-w64-mingw32/64) and Linux (R 4.5.0, x86-64-pc-linux-gnu platform).
All builds to date are done using Rust 1.65.

### Requirements

R 4.2 or greater is required for all installations. R 4.5.0 is preferred.

On Linux or to build from source, you will need these additional dependencies:

- Cargo, the Rust package manager
- Rustc
- xz

And to run the examples in the README or benchmarking scripts, the following are required:

- `dplyr`
- `fuzzyjoin`
- `qdapDictionaries`
- `microbenchmark`

### Installation

Installing from binary is the easiest method on Windows, as it skips the need for the Rust toolchain.
Installing from source is the only officially supported option on Linux systems currently.

#### From binary (Windows only)

Binaries are found in the [releases](https://github.com/JonDDowns/fozziejoin/releases) section.
Currently, binaries are built for R 4.4.3. This binary is confirmed to work on R 4.3.1.
We will not be actively supporting other R versions at this time, as our primary target is an eventual CRAN release.
Please consider installing from source.

```
install.packages('https://github.com/JonDDowns/fozziejoin/releases/download/v0.0.7/fozziejoin_0.0.7', type='win.binary')
```

#### From source

```
install.packages('https://github.com/JonDDowns/fozziejoin/archive/refs/tags/v0.0.7.tar.gz', type='source')
# Alternative: use devtools
# devtools::install_github('https://github.com/JonDDowns/fozziejoin')
```

### Usage

Code herein is adapted from the motivating example used in the `fuzzyjoin` package.
First, we take a list of common misspellings (and their corrected alternatives) from Wikipedia.
To run in a a reasonable amount of time, we take a random sample of 1000.

```{r}
library(dplyr)
library(fozziejoin)
library(fuzzyjoin) # For misspellings dataset

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

Then, we run our join function.

```{r}
fozzie <- fozzie_join(
    sub_misspellings, words, method='lv', by = c('misspelling', 'word'), max_distance=2
)
```

## Benchmarks

To date, `fozziejoin` has been benchmarked on Windows and Linux.
Currently all algorithms except for `soundex` joins have been implemented.
As of v0.0.7, `fozziejoin` beats the equivalent `fuzzyjoin` benchmark in every instance while producing identical results.
The highest observed performance gains come from Linux systems, presumably due to the relative efficiency of parallelization via `rayon`.

[![Linux benchmark results](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Linux.svg)](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Linux.svg)


[![Windows benchmark results](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Windows.svg)](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Windows.svg)

## Known behavior changes relative to `fuzzyjoin`

Items beginning with an exclamation (!) are very likely to be updated for better alignment with `fuzzyjoin` in future releases.

- Matching on columns with `NA` values throw an error in `fuzzyjoin` but simply do not match in `fozziejoin`. 
This allows for NA values to persist in left, right, anti, semi, and full joins without matching all `NA` values to one another.
`NA` will not be considered a matching value in any join type.

- Jaro-Winkler distance
    - The prefix scaling factor (`max_prefix`) is an integer representing a fixed number of characters. The analagous `stringdist` parameter, `bt`, was a proportion of string length.
- ! `fozziejoin` always assigns the suffix ".x" to columns from the LHS and ".y" to columns from the RHS. `fuzzyjoin` only does this when both LHS and RHS contain the same column name. `fozziejoin` may conform to the `fuzzyjoin` behavior in the future.
- ! `fuzzyjoin` returns a `tibble`, while `fozziejoin` currently returns a base `data.frame`. In future releases, `fozziejoin` will add some support for returning `tibble`. I'd like to explore ways to make `tibble` an optional import rather than a required dependency.

## Acknowledgements

- The `extendr` team. This project would not be possible without their great project.
- The `fuzzyjoin` and `stringdist` packages in R. Much of the project is meant to replicate their APIs and special cases handling. `stringdist` is insanely performant.
- The `textdistance` Rust crate `textdistance` is used in many algorithms, and their implementation was referenced to adapt custom string distance algorithms for this project. I have added header comments in all such cases where I adapted the `textdistance` crate without importing it.
- The `rapidfuzz` Rust crate. They do not have all the necessary algorithms implemented, but the ones that they have implemented are very performant.
- The `rayon` Rust crate, which enables efficient parallel data processing.
