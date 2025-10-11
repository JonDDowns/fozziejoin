# fozziejoin 🧸

> ⚠️ **Note**: This project is in early development.
> APIs may change, and installing from source requires the Rust toolchain.

`fozziejoin` is an R package that performs fast fuzzy joins using Rust as a
backend. It is a performance-minded re-imagining of the very popular
[`fuzzyjoin` package]( https://CRAN.R-project.org/package=fuzzyjoin).
Performance improvements relative to `fuzzyjoin` can be significant, especially
for string distance joins. See the [benchmarks](#Benchmarks) for more details.

Currently, the following function families are available:

- `fozzie_string_join` 
- `fozzie_difference_join`
- `fozzie_distance_join`
- `fozzie_interval_join`
- `fozzie_interval_join`
- `fozzie_regex_join`
- `fozzie_temporal_join`
- `fozzie_temporal_interval_join`

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

R 4.2 or greater is required for all installations. R 4.5.0 or greater is preferred.

On Linux or to build from source, you will need these additional dependencies:

- Cargo, the Rust package manager
- Rustc
- xz
- `devtools`: The installation instructions assume you have this package for source installs. `R CMD INSTALL` or even `install.packages()` should also work if you want to avoid this dependency.

To run the examples in the README or benchmarking scripts, the following are
required:

- `dplyr`
- `fuzzyjoin`
- `qdapDictionaries`
- `microbenchmark`
- `tibble`
 
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
# install.packages("https://github.com/JonDDowns/fozziejoin/archive/refs/tags/v0.0.9.tar.gz", type = "source")
```

##### Windows users

To compile Rust extensions for R on Windows (such as those used by `rextendr`),
you must use the **GNU Rust toolchain**, not MSVC. This is because R is built
with GCC (via Rtools), and Rust must match that ABI for compatibility.
This assumes you already have Rust installed.

1. Set the default Rust toolchain to GNU:

```sh
# Install the GNU toolchain if needed
# rustup install stable-x86_64-pc-windows-gnu

rustup override set stable-x86_64-pc-windows-gnu
```

2. Install the latest build from GitHub

```sh
Rscript -e 'devtools::install_github("JonDDowns/fozziejoin")'
# Or, clone and install locally
# git clone https://github.com/JonDDowns/fozziejoin.git
# cd fozziejoin
# Rscript.exe -e "devtools::install()"
```

#### From binary (Windows only)

Binaries are found in the [releases](https://github.com/JonDDowns/fozziejoin/releases)
section. Currently, binaries are built for the current (4.5.1), development
(4.6.0) and most recent old release (4.4.3). Installation has not been tested
on a wide variety of R versions yet. These binaries are provided to encourage
more user testing while we pursue CRAN release, our primary goal. At that
point, the installation process should become easier for most users.

##### Download and extract release bundle

```{r}
# Define GitHub release URL
release_url <- "https://github.com/JonDDowns/fozziejoin/releases/download/v0.0.9/fozziejoin_winbuilds_0.0.9.zip"

# Download the zip file
temp_zip <- tempfile(fileext = ".zip")
download.file(release_url, temp_zip, mode = "wb")

# Extract to a temp directory
extract_dir <- tempfile()
dir.create(extract_dir)
unzip(temp_zip, exdir = extract_dir)
```

#####  Extract the correct version for your system.

Set the `fozzie_version` variable based on your setup. Currently, three build
targets are produced. Choose the one most appropriate for your R version.
Below are suggested versions to use:

- R version 4.6.0 or higher: `r_4.6.0`
- R versions 4.5.0 or higher: `r_4.5.1`
- R versions 4.4.3 and before: `r_4.4.3`

```{r}
# r_folder <- "r_4.6.0"
# r_folder <- "r_4.4.3"
r_folder <- "r_4.5.1"

# Path to the correct subfolder
pkg_path <- file.path(extract_dir, "fozziejoin_winbuilds_0.0.9", r_folder)

# Find the compiled binary (assumes only one .zip inside each subfolder)
pkg_file <- list.files(pkg_path, pattern = "\\.zip$", full.names = TRUE)

# Install the package
install.packages(pkg_file, repos = NULL, type = "win.binary")
```

### Usage

Code herein is adapted from the motivating example used in the `fuzzyjoin`
package. First, we take a list of common misspellings (and their corrected
alternatives) from Wikipedia. To run in a a reasonable amount of time, we
take a random sample of 1000.

```r
library(fozziejoin)
library(tibble)
library(fuzzyjoin) # For misspellings dataset

# Load misspelling data
data(misspellings)

# Take subset of 1k records
set.seed(2016)
sub_misspellings <- misspellings[sample(nrow(misspellings), 100), ]
```

Next, we load a dictionary of words from the `qdapDictionaries` package.

```r
library(qdapDictionaries) # For dictionary
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

Select benchmark comparisons are below. See [the scripts subfolder](./scripts/)
and the [./.github/workflows/run_all_benches.yml](GitHub Actions Workflow)
for the benchmarking implementation. More detailed methods will be
added later via an R vignette. Linux users will observe the largest
performance gains, presumably due to the relative efficiency of parallelization
via `rayon`.


[![Fozziejoin vs. fuzzyjoin runtime on select join methods](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/fozzie_bench_plot.png)](https://raw.githubusercontent.com/JonDDowns/fozziejoin/refs/heads/main/outputs/fozzie_bench_plot.png)

## Known behavior changes relative to `fuzzyjoin`

While `fozziejoin` is heavily inspired by `fuzzyjoin`, it does not seek to
replicate it's behavior entirely. Please submit a GitHub issue if there are
features you'd like to see! We will prioritize feature support based on
community feedback.

Below are some known differences in behavior that we do not currently plan to
address.

- `fozziejoin` allows `NA` values on the join columns specified for string distance joins. `fuzzyjoin` would throw an error. This change allows `NA` values to persist in left, right, anti, semi, and full joins. Two `NA` values are not considered a match. We find this behavior more desirable in the case of fuzzy joins.

- The prefix scaling factor for Jaro-Winkler distance (`max_prefix`) is an integer limiting the number of prefix characters used to boost similarity. In contrast, the analogous `stringdist` parameter `bt` is a proportion of the string length, making the prefix contribution relative rather than fixed.

- Some `stringdist` arguments are not supported. Implementation is challenging, but not impossible. We could prioritize their inclusion if user demand were sufficient:
    - `useBytes`
    - `weight`
    - `useNames` is not relevant to the final output of the fuzzy join. There is no need to implement this.

- For interval joins, we allow for both `real` and `integer` join types!
    - The integer mode is designed to match the behavior of IRanges, which is used in `fuzzyjoin`. You will need to coerce the join columns to integers to enable this mode.
    - The `real` mode behaves more like `data.table`'s `foverlaps`.
    - An `auto` mode (default) will determine the method to use based on the input column type

- `soundex` implementations differ slightly.
    - Our implementation considers multiple encodings in the case of prefixes prefixes, as is specified in the [National Archives Standard](https://www.archives.gov/research/census/soundex).
    - How consecutive similar letters and consonant separators behave is implemented differently. "Coca Cola" would match to "cuckoo" only in our system, while "overshaddowed" and "overwrought" would only match in theirs.

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
