test_that("inner interval join returns overlapping rows", {
  df1 <- data.frame(start = c(1, 5, 10, 30, 15), end = c(3, 7, 12, 32, 25))
  df2 <- data.frame(start = c(2, 6, 11, 33, 100), end = c(4, 8, 13, 35, 125))

  all <- merge(df1, df2, by=NULL)
  overlap <- with(all, (start.x <= end.y & start.y <= end.x))
  expected <- all[overlap,]
  rownames(expected) <- NULL

  result <- fozzie_interval_join(
    df1, df2,
    by = c("start" = "start", "end" = "end"),
    interval_mode = "real"
  )
  expect_identical(result, expected)
})

test_that("left interval join includes all rows from df1", {
  df1 <- data.frame(start = c(1, 5, 10), end = c(3, 7, 12))
  df2 <- data.frame(start = c(2, 6), end = c(4, 8))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), how = "left")
  expect_equal(nrow(result), 3)
  expect_true(any(is.na(result$start.y)))
})

test_that("right interval join includes all rows from df2", {
  df1 <- data.frame(start = c(1, 5), end = c(3, 7))
  df2 <- data.frame(start = c(2, 6, 10), end = c(4, 8, 12))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), how = "right")
  expect_equal(nrow(result), 3)
  expect_true(any(is.na(result$start.x)))
})

test_that("anti interval join returns non-overlapping rows from df1", {
  df1 <- data.frame(start = c(1, 5, 10), end = c(3, 7, 12))
  df2 <- data.frame(start = c(2, 6), end = c(4, 8))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), how = "anti")
  expect_equal(nrow(result), 1)
  expect_equal(result$start, 10)
})

test_that("full interval join includes all rows from both tables", {
  df1 <- data.frame(start = c(1, 5, 10), end = c(3, 7, 12))
  df2 <- data.frame(start = c(100, 101, 102), end = c(101, 102, 103))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), how = "full")
  expect_equal(nrow(result), 6)
})

test_that("overlap_type = 'within' only matches fully contained intervals", {
  df1 <- data.frame(start = c(1, 5), end = c(10, 7))
  df2 <- data.frame(start = c(2, 6), end = c(9, 6.5))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), overlap_type = "within")
  expect_equal(nrow(result), 1)
})

test_that("overlap_type = 'start' only matches overlapping starts", {
  df1 <- data.frame(start = c(1, 5), end = c(10, 7))
  df2 <- data.frame(start = c(1, 6), end = c(2, 8))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), overlap_type = "start")
  expect_equal(nrow(result), 1)
})

test_that("overlap_type = 'end' only matches overlapping ends", {
  df1 <- data.frame(start = c(1, 5), end = c(10, 7))
  df2 <- data.frame(start = c(9, 6), end = c(10, 7))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), overlap_type = "end")
  expect_equal(nrow(result), 2)
})

test_that("maxgap filters out distant overlaps", {
  df1 <- data.frame(start = c(1, 5), end = c(3, 7))
  df2 <- data.frame(start = c(10, 20), end = c(12, 22))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), maxgap = 1)
  expect_equal(nrow(result), 0)
})

test_that("minoverlap filters out short overlaps", {
  df1 <- data.frame(start = c(1, 5), end = c(3, 7))
  df2 <- data.frame(start = c(2.9, 6.9), end = c(3.1, 7.1))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), minoverlap = 0.5)
  expect_equal(nrow(result), 0)
})

test_that("interval_mode = 'integer' works with integer columns", {
  df1 <- data.frame(start = c(1L, 5L), end = c(3L, 7L))
  df2 <- data.frame(start = c(2L, 6L), end = c(4L, 8L))
  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), interval_mode = "integer", maxgap = 0)
  expect_equal(nrow(result), 3)
})

test_that("interval_mode = 'real' works with numeric columns", {
  df1 <- data.frame(start = c(1.0, 5.0), end = c(3.0, 7.0))
  df2 <- data.frame(start = c(2.0, 6.0), end = c(4.0, 8.0))

  result <- fozzie_interval_join(df1, df2, by = c(start = "start", end = "end"), interval_mode = "real")
  expect_equal(nrow(result), 2)
})

test_that("named list for `by` works with interval join", {
  df1 <- data.frame(a = c(1, 5), b = c(3, 7))
  df2 <- data.frame(c = c(2, 6), d = c(4, 8))

  result <- fozzie_interval_join(df1, df2, by = list(a = "c", b = "d"))
  expect_equal(nrow(result), 2)
})

test_that("interval_mode = real handles integer inputs", {
  df1 <- data.frame(
    start = c(100.0, 200.5, 300.2, 400.0),
    end   = c(105.0, 210.0, 305.0, 410.0)
  )

  df2 <- data.frame(
    start = c(102.0, 205.0, 299.0, 405.0),
    end   = c(106L, 209L, 304L, 415L)
  )

  olaps <- fozzie_interval_join(
    df1, df2,
    by = c(start = "start", end = "end"),
    how = "inner",
  )

  expect_equal(nrow(olaps), 4)
})

