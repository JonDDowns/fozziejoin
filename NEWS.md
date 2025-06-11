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
