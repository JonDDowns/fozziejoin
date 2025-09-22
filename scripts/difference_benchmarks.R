# Load required packages
library(fuzzyjoin)
library(microbenchmark)
library(dplyr)
devtools::install()
library(fozziejoin)

# Set seed for reproducibility
set.seed(123)

# Define size
samp_sizes <- c(100, 1000, 5000, 10000, 20000, 31623)
for (n in samp_sizes) {
  print(paste0("n = ", n))

  df1 <- data.frame(X = rnorm(n))
  df2 <- data.frame(X = df1$X + rnorm(n, sd = 0.05))

  fozzie_difference_join2 <- function(df1, df2, max_distance = 0.1) {
    fozzie_difference_join(
      df1 = df1,
      df2 = df2,
      by = list("X" = "X"),
      how = "inner",
      max_distance = max_distance,
      distance_col = "dist",
    )
  }

  # Define wrapper for fuzzyjoin
  fuzzyjoin_difference <- function(df1, df2, max_distance = 0.1) {
    difference_join(
      df1,
      df2,
      by = "X",
      max_dist = max_distance,
      distance_col = "dist",
      mode = "inner"
    )
  }

  # Run benchmark
  print(runtime <- microbenchmark(
    fozzie = foz <- fozzie_difference_join2(df1, df2),
    #fuzzy = fuzz <- fuzzyjoin_difference(df1, df2),
    times = 2
  ))
  # Compute ratio of median times
  #ratio <- median(runtime$time[runtime$expr == "fuzzy"]) /
  #			median(runtime$time[runtime$expr == "fozzie"])

  #	print(ratio)
  #	print(nrow(foz) == nrow(fuzz))
}
