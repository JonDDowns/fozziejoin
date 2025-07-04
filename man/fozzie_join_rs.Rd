% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/extendr-wrappers.R
\name{fozzie_join_rs}
\alias{fozzie_join_rs}
\title{Fuzzy Record Linkage via String Similarity Matching}
\usage{
fozzie_join_rs(
  df1,
  df2,
  by,
  method,
  how,
  max_distance,
  distance_col,
  q,
  max_prefix,
  prefix_weight,
  nthread
)
}
\arguments{
\item{df1}{A data frame (\code{List}) representing the left input.}

\item{df2}{A data frame (\code{List}) representing the right input.}

\item{by}{A named list mapping columns from \code{df1} to corresponding columns in \code{df2}.}

\item{method}{A string specifying the string similarity or distance metric to use. Options include:
\itemize{
\item \code{"levenshtein"}, \code{"lv"}: Levenshtein edit distance
\item \code{"osa"}: Optimal string alignment
\item \code{"damerau_levensthein"}, \code{"dl"}: Damerau-Levenshtein edit distance
\item \code{"hamming"}: Hamming distance (requires equal-length strings)
\item \code{"lcs"}: Longest common subsequence
\item \code{"qgram"}, \code{"cosine"}, \code{"jaccard"}: Q-gram-based similarity (requires \code{q})
\item \code{"jaro_winkler"}, \code{"jw"}: Jaro-Winkler similarity (uses \code{max_prefix} and \code{prefix_weight})
}}

\item{how}{Type of join to perform. Options are:
\itemize{
\item \code{"inner"} (default): Matches only
\item \code{"left"}: All rows from \code{df1} with matches in \code{df2}
\item \code{"right"}: All rows from \code{df2} with matches in \code{df1}
\item \code{"anti"}: Unmatched rows from \code{df1}
\item \code{"full"}: All matches from both sides
}}

\item{max_distance}{A numeric threshold for allowable distance or dissimilarity.}

\item{distance_col}{Optional string specifying the name of a column to include distance/similarity values.}

\item{q}{Optional integer specifying q-gram size (required for \code{"qgram"}, \code{"cosine"}, and \code{"jaccard"}).}

\item{max_prefix}{Optional integer for Jaro-Winkler: maximum prefix length for boosting.}

\item{prefix_weight}{Optional numeric for Jaro-Winkler: weight multiplier for shared prefix boost.}

\item{nthread}{Optional number of threads to use (defaults to all available cores if \code{NULL}).}
}
\value{
A data frame (\code{Robj}) containing joined rows with \code{.x} and \code{.y} suffixes from \code{df1} and \code{df2}, respectively.
If \code{distance_col} is specified, an additional numeric column stores the computed similarity or distance.
}
\description{
Performs approximate joins between two R data frames (lists) using fuzzy string matching.
This function computes pairwise string distances between join keys using edit distance or
similarity metrics, supporting parallel execution and key-wise filtering.
}
\details{
Internally, string distances are computed using efficient hash maps and parallel search over character vectors.
The function supports multi-column joins by filtering progressively across keys and computing distances at each level.

This is an internal low-level wrapper used by high-level join interfaces such as \code{fozzie_join()}.
}
\seealso{
\code{\link[=fozzie_join]{fozzie_join()}}, \code{\link[=fozzie_inner_join]{fozzie_inner_join()}}, \code{\link[=fozzie_left_join]{fozzie_left_join()}}
}
