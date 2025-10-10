# fozziejoin 0.0.10

- Two vignettes added:
    - General package overview
    - Benchmarking sample and considerations
- Interval join with `interval_mode = 'real'` now handles a mix of integer and
  double inputs correctly.
- If `by = NULL`, the internal `common_by` function will now print the columns
  used in the join.
- License information updated to reflect author(s) of all imported Rust crates.
  This seems necessary based on a review of other similar `extendr` packages.
- Reproducible benchmark scripts created using github workflows
- Users can now set a global thread count via `options(fozzie.nthread = 4)`,
  which will be respected by all functions with an nthread argument. By
  default, the package uses the default from the multithreading Rust library
  `rayon`.

# fozziejoin 0.0.9

- Distance joins now available.
- Semi join type added.
- If one of the input dataframes is a `tibble`, the output result will now be a `tibble`.
    - This is necessary to handle some of the functionalities present in `tibble` but not in `data.frame`.
    - `tibble` is now a suggested import.
- Interval joins added, with three "interval_mode"'s:
    - `integer`: integer-based join types, with behavior designed to emulate IRanges findOverlaps. Importantly, [1, 2] and [3, 4] would be considered overlapping in this case.
    - `real`: real number joins, where there must be some continuous overlap between ranges to be considered matching.
    - `auto`: behavior determined by the input column types.
- The `by` function should now better resemble the `fuzzyjoin` implementation. Notes have been added to the internal function signature to acknowledge their contribution.
- Performance improvements.
    - Rust code now uses FxHashMap and FxHashSet universally.
    - Simplified memory structures for case when only one column is joined on.
- Better code organization in Rust code.
- Better error handling.
    - Most areas of the code now gracefully return an error to R instead of panicking.
    - The areas where panics still might happen aren't known to throw errors, but I'd still like to properly handle them in the future.
- Now using `styler` to be more style guide compliant.

# fozziejoin 0.0.8

- Arbitrary vector attributes, such as factor levels and POSIX dates, should now be supported. See: [Issue #6](https://github.com/JonDDowns/fozziejoin/issues/6). Testing utilities updated to validate this change.
- Fixed a bug in the `nthread` argument wherein the user-specified thread count was ignored and the default global thread pool settings were always used. See [Issue #7](https://github.com/JonDDowns/fozziejoin/issues/7).
- Contributor code of conduct added
- string distance functions added to their own submodule within the Rust code. This is to better organize the code as we plan to add other fuzzy join types (distance, difference, geo, etc.)
- `fozzie_join` functions have been renamed to `fozzie_string_join`. This will better describe the function behavior and allow us to add other join types in the future. See [Issue #9](https://github.com/JonDDowns/fozziejoin/issues/9)
- `fozzie_string_full_join` now implements full joins as the union of the left and right fuzzy join. Before this, it was the cartesian product of left and right datasets.
- `fozzie_difference_join` suite of functions now available. This allows joining on numeric distance.

# fozziejoin 0.0.7

- Switched to `rapidfuzz` crate for supported algorithms, as they perform better than prior implementations.
- README updates
- .gitignore updated to remove vendored packages, as is convention.

# fozziejoin 0.0.6

- Fixed issue with Jaccard and qgram distance (see [issue #3](https://github.com/JonDDowns/fozziejoin/issues/3)).
- Comparative benchmark vs. fozziejoin updated to check for identical output (after some light conversions for consistency in column naming/output object classes).

# fozziejoin 0.0.5

## Functionality and performance updates

- Joins now properly handle dates and factors
- Added convenience function for all directional variants of joins (`fozzie_left_join()`, `fozzie_inner_join()`, ...).
- Reverted a change from v0.0.4 wherein speed distance calculation methods differ by operating system (Windows vs. everything else). The supposed speed gains were actually flaws in the evaluation. Reverted back to a single method for all OS's.
- Speedup in OSA algorithm due to more efficient memory handling.

## Documentation

- README updates:
    - Installation steps reflect current procedures and reference the GitHub release for `v0.0.5`.
    - Requirements updated as there is now an install from binary option for Windows which has fewer system requirements.
    - Removed Todo section. Will use GitHub issues for this sort of thing moving forward.
    - Documentation had error in example usage code. `fuzzyjoin` was a required import for the `misspellings` dataset.
- Documentation updated to pass all `devtools::check()` and `R CMD check` checks for the first time.
- There are a few examples where code is only lightly adapted from the `textdistance` crate implementation. Those scripts now have a header comment acknowledging the original author. 

## Preparation for CRAN release

- This version is the last before attempting CRAN distribution. A GitHub "release" has been created with the package build for all operating systems. CRAN acceptance may require multiple versions.
- All tests now force `nthread=2` for compliance with CRAN policies.

# fozziejoin 0.0.4

- Performance improvements:
    - Windows build now uses a parallelization method more appropriate for the OS (rayon's `par_chunks` have replaced equivalent `par_iter` operations)
    - Q-gram based edit distances have been sped up by reducing memory copies.
- Scripts for benchmarking have been added.
- Project README updated to include some benchmarking results.

# fozziejoin 0.0.3

- Anti join implemented
- Full join implemented
- Multikey joins now allowed (e.g. joining on "Name" and "DOB").
- LCS string distance now available. This matches the original R `stringdist` behavior.
- Can control number of threads using the `nthread` parameter.
- Jaro-Winkler parameters `prefix_weight` and `max_prefix` parameters added. These are similar to the `bt` and `p` parameters in the `stringdist` package, with some differences (`prefix_weight` is a set number of characters, not a proportion).
- The `jaro` method is no longer supported. The default values for the `jw` and `jaro_winkler` methods simplify into the Jaro case.
- Removed case insensitive matching as an immediate project goal.

# fozziejoin 0.0.2

- Right-hand join functionality implemented.
- The parameter `distance_col` is live. It can be used to add the string distance of joined fields to the output.
- Fixed an issue where left and right joins would replace `NA` in R character fields with a string with the string value "NA". Tests updated to expect a true `NA`.
- Added explicit checks for `NA` strings in all Rust internals that perform fuzzy matches. If one or more values in a pair is `NA`, the pair is considered a non-match.
- Updated README.

# fozziejoin 0.0.1

- NEWS.md added
- Inner join implemented for all string distance algorithms except LCS
- Most string distance algorithms have been implemented for `inner` and `left` joins. Results were verified against expectations and with the `fuzzyjoin` package. Exceptions:
	- `jarowinkler`/`jw` method requires the addition of new parameters for `p` and `dt` to be fully customizable. Currently, jaro_winkler defaults to a scaling factor of 0.1 and a maximum prefix of 4. This is consistent with the default of the `stringdist` method. 
	- `jaro` algorithm does not actually exist in the `stringdist` implementation, as it is equivalent to setting `p=0`.
	- LSA algorithm is not implemented yet. There is *an implementation* in the Rust code, but it is not correct and the R user has no way of calling that method.
- Project DESCRIPTION file updated
- `fuzzy_join` API call now includes the `how` method to specify the join type. `inner` and `left` are the currently supported methods. At least `right`, `full`, and `anti` are planned for future releases.
