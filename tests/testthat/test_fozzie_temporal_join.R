test_that("temporal inner join matches within seconds", {
  df1 <- data.frame(time = as.POSIXct(c("2023-01-01 12:00:00", "2023-01-01 13:00:00")))
  df2 <- data.frame(time = as.POSIXct(c("2023-01-01 12:00:05", "2023-01-01 14:00:00")))

  result <- fozzie_temporal_inner_join(df1, df2, by = c("time"), max_distance = 10, unit = "seconds")
  expect_equal(nrow(result), 1)
  expect_equal(result$time.x, as.POSIXct("2023-01-01 12:00:00"))
  expect_equal(result$time.y, as.POSIXct("2023-01-01 12:00:05"))
})

test_that("temporal inner join matches within seconds on POSIXlt", {
  df1 <- data.frame(time = as.POSIXlt(c("2023-01-01 12:00:00", "2023-01-01 13:00:00")))
  df2 <- data.frame(time = as.POSIXlt(c("2023-01-01 12:00:05", "2023-01-01 14:00:00")))

  result <- fozzie_temporal_inner_join(df1, df2, by = c("time"), max_distance = 10, unit = "seconds")
  expect_equal(nrow(result), 1)
  expect_equal(result$time.x, as.POSIXct("2023-01-01 12:00:00"))
  expect_equal(result$time.y, as.POSIXct("2023-01-01 12:00:05"))
})

test_that("temporal join works with Date columns and unit = 'days'", {
  df1 <- data.frame(date = as.Date(c("2023-01-01", "2023-01-04", "2023-01-07")))
  df2 <- data.frame(date = as.Date(c("2023-01-02", "2023-01-05", "2023-01-08")))

  # max_distance = 1 day should match each df1 row with the next df2 row
  result <- fozzie_temporal_inner_join(df1, df2, by = c(date = "date"), max_distance = 1, unit = "days")

  expect_equal(nrow(result), 3)
  expect_equal(as.integer(abs(result$date.y - result$date.x)), rep(1, 3))

  expect_error(fozzie_temporal_inner_join(
    df1, df2,
    by = c(date = "date"), max_distance = 1, unit = "hours"
  ))
})
