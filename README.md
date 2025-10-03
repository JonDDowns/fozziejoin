# fozziejoin 🧸

> ⚠️ **Note**: This project is in early development.
> APIs may change, and installing from source requires the Rust toolchain.

`fozziejoin` is an R package that performs fast fuzzy joins using Rust as a
backend. It is meant to be a performance-minded version of the
[`fuzzyjoin` package]( https://CRAN.R-project.org/package=fuzzyjoin).
Performance improvements relative to `fuzzyjoin` can be significant, especially
for string distance joins. See the [benchmarks][#Benchmarks] for more details.

Currently, the following function families are available:

- `fozzie_string_join` 
- `fozzie_difference_join`
- `fozzie_distance_join`

These function families include related functions, such as 
`fozzie_string_inner_join`.

The name is a playful nod to “fuzzy join” — reminiscent of 
[Fozzie Bear](https://en.wikipedia.org/wiki/Fozzie_Bear) from the Muppets.
A picture of Fozzie will appear in the repo once the legal team gets braver.
**Wocka wocka!**

## Getting started

Code has been written on a combination of Windows (R 4.3.2, 
x86_64-w64-mingw32/64) and Linux (R 4.5.1, x86-64-pc-linux-gnu platform).

### Requirements

R 4.2 or greater is required for all installations. R 4.5.0 is preferred.

On Linux or to build from source, you will need these additional dependencies:

- Cargo, the Rust package manager
- Rustc
- xz

To run the examples in the README or benchmarking scripts, the following are
required:

- `dplyr`
- `fuzzyjoin`
- `qdapDictionaries`
- `microbenchmark`

### Installation

`fozziejoin` is currently under active development. The recommended
installation method is from source. Precompiled binaries for select Windows
builds will be provided with each tagged release. Once the package is accepted
to CRAN, binaries will be available across platforms and R versions. Until 
then, our focus is on building a stable, CRAN-ready product.

#### From source

We recommend installing from the main GitHub branch for the latest updates.
The main branch is only updated when all tests are passing.

##### Linux

macOS is expected to work but is not yet officially tested.

```r
devtools::install_github("JonDDowns/fozziejoin")

# Alternatively, install a tagged release:
# install.packages("https://github.com/JonDDowns/fozziejoin/archive/refs/tags/v0.0.8.tar.gz", type = "source")
```

##### Windows users

To compile Rust extensions for R on Windows (such as those used by `rextendr`),
you must use the **GNU Rust toolchain**, not MSVC. This is because R is built
with GCC (via Rtools), and Rust must match that ABI for compatibility.
This assumes you already have Rust installed.

1. Clone the repo and change into it

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
devtools::install()
# Or install directly from GitHub:
# devtools::install_github("JonDDowns/fozziejoin")
```

#### From binary (Windows only)

Binaries are found in the [releases](https://github.com/JonDDowns/fozziejoin/releases)
section. Currently, binaries are built for R 4.4.3. This binary is confirmed to
work on R 4.3.1. We will not be actively supporting other R versions at this
time, as our primary target is an eventual CRAN release. Please consider 
installing from source.

```
install.packages('https://github.com/JonDDowns/fozziejoin/releases/download/v0.0.8/fozziejoin_0.0.8', type='win.binary')
```

### Usage

Code herein is adapted from the motivating example used in the `fuzzyjoin`
package. First, we take a list of common misspellings (and their corrected
alternatives) from Wikipedia. To run in a a reasonable amount of time, we
take a random sample of 1000.

```r
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

```r
# Use the dictionary of words from the qdapDictionaries package,
# which is based on the Nettalk corpus.
library(qdapDictionaries)
words <- tibble::as_tibble(DICTIONARY)
```

Then, we run our join function.

```r
fozzie <- fozzie_string_join(
    sub_misspellings, words, method='lv', 
    by = c('misspelling' = 'word'), max_distance=2
)
```

## Benchmarks

To date, `fozziejoin` has been benchmarked on Windows and Linux.
Currently all string distance algorithms except for `soundex` joins have been
implemented. As of v0.0.7, `fozziejoin` beats the equivalent `fuzzyjoin`
benchmark in every instance while producing identical results. The highest
observed performance gains come from Linux systems, presumably due to the
relative efficiency of parallelization via `rayon`. All benchmark scripts are
located [in the scripts subfolder](./scripts/benchmarks.R).

[![Linux benchmark results](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Linux.svg)](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Linux.svg)

[![Windows benchmark results](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Windows.svg)](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/benchmark_plot_Windows.svg)

## Known behavior changes relative to `fuzzyjoin`

While `fozziejoin` is heavily inspired by `fuzzyjoin`, it does not seek to
replicate it's behavior entirely. A list of planned changes to better align
with `fuzzyjoin` behavior are listed in [this GH issue](https://github.com/JonDDowns/fozziejoin/issues/5).
Please submit a GitHub issue if there are features you'd like to see! We will
prioritize feature support based on community feedback.

Below are some behavior changes that we do not currently plan to address.

- `fozziejoin` allows `NA` values on the join columns specified for string distance joins. `fuzzyjoin` would throw an error. This change allows `NA` values to persist in left, right, anti, semi, and full joins. Two `NA` values are not considered a match.

- The prefix scaling factor for Jaro-Winkler distance (`max_prefix`) is an integer representing a fixed number of characters. The analagous `stringdist` parameter, `bt`, was a proportion of string length.

- Some `stringdist` arguments are not supported. Implementation is challenging, but not impossible. We could prioritize their inclusion if user demand were sufficient:
    - `useBytes`
    - `weight`

## Acknowledgements

- The `extendr` team. This project would not be possible without their great project.
- The `fuzzyjoin` package. Much of the project is meant to replicate their APIs and special cases handling.
- `stringdist` was used as a source of truth when developing string distance algorithms. `stringdist` is insanely performant.
- The `textdistance` Rust crate is used in many algorithms, and their implementation was referenced to adapt custom string distance algorithms for this project. Such instances are explicitly acknowledged in the source code.
- The `rapidfuzz` Rust crate. When available, we tend to use `rapidfuzz` string distance algorithms due to its stellar performance.
- The `rayon` Rust crate, which enables efficient parallel data processing.

## Contributions Welcome

We welcome contributions of all kinds- whether it's improving documentation,
reporting issues, or submitting pull requests. Your input helps make this 
project better for everyone.

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating, you agree to uphold its standards of respectful and inclusive
behavior.

If you experience or witness any problematic behavior, please [contact me via
GitHub](https://github.com/JonDDowns) or at the email listed in the DESCRIPTION
file.
