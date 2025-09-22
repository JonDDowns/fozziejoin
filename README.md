# fozziejoin 🧸

> ⚠️ **Note**: This project is in early development.
> APIs may change, and installing from source requires the Rust toolchain.

`fozziejoin` is an R package that performs fast fuzzy joins using Rust as a backend. It is meant to be a performance-minded version of the [fuzzyjoin package](https://github.com/dgrtwo/fuzzyjoin). Speedups can be significant: see our benchmarks section.

`fozziejoin` currently has two fuzzy algorithms implemented: 

- `fozzie_string_join`: join two data frames together by string distance algorithms (Hamming, Levenshtein, etc.)
- `fozzie_distance_join`: join two data frames together by numeric distance.
- `fozzie_interval_join`: join two dataframes together by any interval overlap

As one example of where performance gains come from, consider the case of string
distance joins.`fozziejoin` saves memory by filtering out any results that are
not within the user-defined threshold before returning a result to R.
`fozziejoin` relies on the `stringdist` library, which requires all pairwise
distances to be returned as an intermediate object.

The name is a playful nod to “fuzzy join” — reminiscent of [Fozzie Bear](https://en.wikipedia.org/wiki/Fozzie_Bear)
from the Muppets. A picture of Fozzie will appear in the repo once the legal
team gets braver. **Wocka wocka!**

## Getting started

Code has been written on a combination of Windows (R 4.3.2, 
x86_64-w64-mingw32/64) and Linux (R 4.5.0, x86-64-pc-linux-gnu platform). All
builds to date are done using Rust 1.65.

### Requirements

R 4.2 or greater is required for all installations. R 4.5.0 is preferred.

On Linux or to build from source, you will need these additional dependencies:

- Cargo, the Rust package manager
- Rustc
- xz

And to run the examples in the README or benchmarking scripts, the following
are required:

- `dplyr`
- `fuzzyjoin`
- `qdapDictionaries`
- `microbenchmark`

### Installation

Installing from binary is the easiest method on Windows, as it skips the need
for the Rust toolchain. Installing from source is the only officially supported
option on Linux and MacOS systems currently. The package has not been tested on
MacOS.

#### From binary (Windows only)

Binaries are found in the [releases](https://github.com/JonDDowns/fozziejoin/releases) section.
Currently, binaries are built for R 4.4.3. This binary is confirmed to work on R 4.3.1.
We will not be actively supporting other R versions at this time, as our primary target is an eventual CRAN release.
Please consider installing from source.

```
install.packages('https://github.com/JonDDowns/fozziejoin/releases/download/v0.0.8/fozziejoin_0.0.8', type='win.binary')
```

#### From source

#### Windows users

To compile Rust extensions for R on Windows (such as those used by `rextendr`),
you must use the **GNU Rust toolchain**, not MSVC. This is because R is built
with GCC (via Rtools), and Rust must match that ABI for compatibility.
This assumes you already have Rust installed.

1. Clone the repo

```sh
git clone https://github.com/JonDDowns/fozziejoin.git
cd fozziejoin
```

2. Set the default Rust toolchain to GNU:

```sh
# Install the GNU toolchain if needed
# rustup install stable-x86_64-pc-windows-gnu

rustup override set stable-x86_64-pc-windows-gnu
```

3. Install the package:

```r
install.packages('https://github.com/JonDDowns/fozziejoin/archive/refs/tags/v0.0.8.tar.gz', type='source')
# Alternative: use devtools
# devtools::install_github('JonDDowns/fozziejoin')
```

##### Linux

This should also work on MacOS, but is not currently tested.

```r
install.packages('https://github.com/JonDDowns/fozziejoin/archive/refs/tags/v0.0.8.tar.gz', type='source')
# Alternative: use devtools
# devtools::install_github('JonDDowns/fozziejoin')
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
fozzie <- fozzie_string_join(
    sub_misspellings, words, method='lv', by = c('misspelling', 'word'), max_distance=2
)
```

## Benchmarks

To date, `fozziejoin` has been benchmarked on Windows and Linux. Currently all
algorithms except for `soundex` joins have been implemented. As of v0.0.7,
`fozziejoin` beats the equivalent `fuzzyjoin` benchmark in every instance while
producing identical results. The highest observed performance gains come from
Linux systems, presumably due to the relative efficiency of parallelization via
`rayon`. Benchmark scripts are located [here](./scripts/).

[![Linux benchmark results](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Linux.svg)](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Linux.svg)


[![Windows benchmark results](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Windows.svg)](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Windows.svg)

## Known behavior changes relative to `fuzzyjoin`

While `fozziejoin` is heavily inspired by `fuzzyjoin`, it does not seek to
replicate it's behavior entirely. A list of planned changes to better align
with `fuzzyjoin` behavior are listed in [this GH issue](https://github.com/JonDDowns/fozziejoin/issues/5).
Below are some behavior changes that we do not currently plan to address.
Please submit a GitHub issue if you think these should change!

- Matching on columns with `NA` values throw an error in `fuzzyjoin` but simply do not match in `fozziejoin`. This allows for `NA` values to persist in left, right, anti, semi, and full joins without matching all `NA` values to one another. `NA` will not be considered a matching value in any join type.

- The prefix scaling factor for Jaro-Winkler distance (`max_prefix`) is an integer representing a fixed number of characters. The analagous `stringdist` parameter, `bt`, was a proportion of string length.

- Some `stringdist` arguments are not supported. Implementation is challenging, but not impossible. We could prioritize their inclusion if user demand were sufficient:
    - `useBytes`
    - `weight`

## Acknowledgements

- The `extendr` team. This project would not be possible without their great project.
- The `fuzzyjoin` and `stringdist` packages in R. Much of the project is meant to replicate their APIs and special cases handling. `stringdist` is insanely performant.
- The `textdistance` Rust crate `textdistance` is used in many algorithms, and their implementation was referenced to adapt custom string distance algorithms for this project. I have added header comments in all such cases where I adapted the `textdistance` crate without importing it.
- The `rapidfuzz` Rust crate. They do not have all the necessary algorithms implemented, but the ones that they have implemented are very performant.
- The `rayon` Rust crate, which enables efficient parallel data processing.

## Contributions Welcome

We welcome contributions of all kinds- whether it's improving documentation, reporting issues, or submitting pull requests. Your input helps make this project better for everyone.

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you agree to uphold its standards of respectful and inclusive behavior.

If you experience or witness any problematic behavior, please [contact me via GitHub](https://github.com/JonDDowns) or open a private issue if appropriate.
