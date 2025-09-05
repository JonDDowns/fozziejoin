# fozziejoin 0.0.7 (WORKING)

- Switched to `rapidfuzz` crate for supported algorithms, as they perform better than prior implementations.
- README updates
- .gitignore updated to remove vendored packages, as is convention.

# fozziejoin 0.0.6 (WORKING)

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
