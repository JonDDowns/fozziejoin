test_that("temporal interval inner join matches overlapping Date intervals", {
  df1 <- data.frame(
    start = as.Date(c("2023-01-01", "2023-01-05")),
    end = as.Date(c("2023-01-03", "2023-01-07"))
  )
  df2 <- data.frame(
    start = as.Date(c("2023-01-02", "2023-01-06")),
    end = as.Date(c("2023-01-04", "2023-01-08"))
  )

  result <- fozzie_temporal_interval_inner_join(
    df1, df2,
    by = list(start = "start", end = "end"),
    overlap_type = "any",
    maxgap = 0,
    minoverlap = 0,
    unit = "days"
  )

  expect_equal(nrow(result), 2)
})

test_that("temporal interval join handles POSIXct with second gap", {
  df1 <- data.frame(
    start = as.POSIXct("2023-01-01 14:00:01"),
    end = as.POSIXct("2023-01-01 14:00:03")
  )
  df2 <- data.frame(
    start = as.POSIXct("2023-01-01 13:00:00"),
    end = as.POSIXct("2023-01-01 14:00:00")
  )

  result <- fozzie_temporal_interval_inner_join(
    df1, df2,
    by = c(start = "start", end = "end"),
    maxgap = 1,
    unit = "seconds"
  )

  expect_equal(nrow(result), 1)
})

test_that("temporal interval join handles POSIXct with minute gap", {
  df1 <- data.frame(
    start = as.POSIXct("2023-01-01 14:01:00"),
    end = as.POSIXct("2023-01-01 14:02:00")
  )
  df2 <- data.frame(
    start = as.POSIXct("2023-01-01 13:00:00"),
    end = as.POSIXct("2023-01-01 14:00:00")
  )
  result <- fozzie_temporal_interval_inner_join(
    df1, df2,
    by = c(start = "start", end = "end"),
    maxgap = 1,
    unit = "minutes"
  )

  expect_equal(nrow(result), 1)
})

test_that("temporal interval join fails on mixed Date and POSIXct", {
  df1 <- data.frame(start = as.Date("2023-01-01"), end = as.Date("2023-01-03"))
  df2 <- data.frame(start = as.POSIXct("2023-01-01"), end = as.POSIXct("2023-01-03"))

  expect_error(
    fozzie_temporal_interval_inner_join(df1, df2, by = list(start = "start", end = "end")),
    "Join columns must be of the same type"
  )
})

test_that("temporal interval left join includes unmatched rows", {
  df1 <- data.frame(start = as.Date("2023-01-01"), end = as.Date("2023-01-03"))
  df2 <- data.frame(start = as.Date("2023-01-10"), end = as.Date("2023-01-12"))

  result <- fozzie_temporal_interval_left_join(
    df1, df2,
    by = list(start = "start", end = "end"),
    unit = "days"
  )

  expect_equal(nrow(result), 1)
  expect_true(is.na(result$start.y))
})
