library(tibble)

test_that("fozzie_difference_join returns a tibble", {
  x <- tibble(a = 1:3, b = 4:6)
  y <- tibble(a = 2:4, b = 5:7)
  result <- fozzie_difference_join(x, y , by = c('a', 'b'))
  expect_s3_class(result, "tbl_df")
})

test_that("fozzie_distance_join returns a tibble", {
  x <- tibble(a = c(47.6, 48.0), b = c(-122.3, -122.5))
  y <- tibble(a = c(47.7, 48.1), b = c(-122.4, -122.6))
  result <- fozzie_distance_join(x, y, by = c('a', 'b'))
  expect_s3_class(result, "tbl_df")
})

test_that("fozzie_interval_join returns a tibble", {
  x <- tibble(start = c(1, 5), end = c(3, 7))
  y <- tibble(start = c(2, 6), end = c(4, 8))
  result <- fozzie_interval_join(x, y, by = c('start', 'end'))
  expect_s3_class(result, "tbl_df")
})

test_that("fozzie_string_join returns a tibble", {
  x <- tibble(name = c("apple", "banana"))
  y <- tibble(name = c("appl", "banan"))
  result <- fozzie_string_join(x, y, by = c('name'))
  expect_s3_class(result, "tbl_df")
})

test_that("fozzie_temporal_interval_join returns a tibble", {
  x <- tibble(start = as.POSIXct(c("2023-01-01", "2023-01-02")),
              end = as.POSIXct(c("2023-01-03", "2023-01-04")))
  y <- tibble(start = as.POSIXct(c("2023-01-02", "2023-01-03")),
              end = as.POSIXct(c("2023-01-04", "2023-01-05")))
  result <- fozzie_temporal_interval_join(x, y, by = c('start', 'end'))
  expect_s3_class(result, "tbl_df")
})

test_that("fozzie_temporal_join returns a tibble", {
  x <- tibble(time = as.POSIXct(c("2023-01-01", "2023-01-02")))
  y <- tibble(time = as.POSIXct(c("2023-01-02", "2023-01-03")))
  result <- fozzie_temporal_join(x, y, by = 'time')
  expect_s3_class(result, "tbl_df")
})

