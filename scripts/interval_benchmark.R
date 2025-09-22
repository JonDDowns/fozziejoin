# devtools::install()
library(fuzzyjoin)
library(fozziejoin)
library(microbenchmark)

# Generate synthetic interval data
set.seed(42)
for (n in c(100, 500, 1000, 2000)) {
  print(paste0("n = ", n))
  df1 <- data.frame(
    start = round(runif(n, 0, 1000)),
    end   = round(runif(n, 1000, 2000))
  )
  df1 <- transform(df1, start = pmin(start, end), end = pmax(start, end))

  df2 <- data.frame(
    start = round(runif(n, 500, 1500)),
    end   = round(runif(n, 1500, 2500))
  )
  df2 <- transform(df2, start = pmin(start, end), end = pmax(start, end))

  # Join keys
  by_keys <- list(start = "start", end = "end")

  # Run benchmark
  print(runtime <- microbenchmark(
    fuzzy = fuzz <- fuzzyjoin::interval_join(
      df1, df2, by = c('start', 'end'), maxgap = 0
    ),
    fozzie = fozz <- fozziejoin::fozzie_interval_join(
      df1, df2, by = by_keys, how = "inner", max_distance = 1,
      distance_col = 'dist', nthread = NULL
    ),
    times = 2
  ))
  ratio <- median(runtime$time[runtime$expr == "fuzzy"]) /
    median(runtime$time[runtime$expr == "fozzie"])

  print(ratio)
  print(nrow(fozz))
  print(nrow(fuzz))

}
