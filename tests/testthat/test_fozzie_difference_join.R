test_that("inner join returns matched rows within threshold", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.0))
  df2 <- data.frame(x = c(1.05, 2.2, 2.95))

  result <- fozzie_difference_inner_join(df1, df2, by = c("x"), max_distance = 0.1)
  expect_equal(nrow(result), 2)
  expect_true(all(abs(result$x.x - result$x.y) <= 0.15))
})

test_that("left join includes all rows from df1", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.0))
  df2 <- data.frame(x = c(1.05, 2.1))

  result <- fozzie_difference_left_join(df1, df2, by = c("x"), max_distance = 0.05)
  expect_equal(nrow(result), 3)
  expect_true(any(is.na(result$x.y)))
})

test_that("right join includes all rows from df2", {
  df1 <- data.frame(x = c(1.0, 2.0))
  df2 <- data.frame(x = c(1.05, 2.1, 3.0))

  result <- fozzie_difference_right_join(df1, df2, by = c("x"), max_distance = 0.05)
  expect_equal(nrow(result), 3)
  expect_true(any(is.na(result$x.x)))
})

test_that("anti join returns unmatched rows from df1", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.0))
  df2 <- data.frame(x = c(1.05, 2.1))

  result <- fozzie_difference_anti_join(df1, df2, by = c("x"), max_distance = 0.05)
  expect_equal(nrow(result), 2)
  expect_equal(result$x, c(2.0, 3.0))
  expect_equal(result$x, c(2.0, 3.0))
})

test_that("full join includes all rows from both tables", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.1))
  df2 <- data.frame(x = c(2.1, 3.0, 4.0))

  result <- fozzie_difference_full_join(df1, df2, by = c("x"), max_distance = 0.05)
  expect_equal(nrow(result), 6)
})

test_that("distance_col is correctly computed", {
  df1 <- data.frame(x = c(1.0))
  df2 <- data.frame(x = c(1.05))

  result <- fozzie_difference_inner_join(df1, df2, by = c("x"), max_distance = 0.1, distance_col = "diff")
  expect_true("diff" %in% names(result))
  expect_equal(result$diff, 0.05)
})

test_that("named list for `by` works", {
  df1 <- data.frame(a = c(1.0))
  df2 <- data.frame(b = c(1.05))

  result <- fozzie_difference_inner_join(df1, df2, by = list(a = "b"), max_distance = 0.1)
  expect_equal(nrow(result), 1)
})

test_that("multi-column inner difference join matches rows across multiple keys", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.0, 4.0), y = c(10.0, 20.0, 30.0, 100.0))
  df2 <- data.frame(x = c(1.05, 2.1, 2.95, 3.95), y = c(10.1, 19.9, 30.05, 1.0))

  result <- fozzie_difference_inner_join(df1, df2, by = c(x = "x", y = "y"), max_distance = 0.15)
  expect_equal(nrow(result), 3)
  expect_true(all(abs(result$x.x - result$x.y) <= 0.15))
  expect_true(all(abs(result$y.x - result$y.y) <= 0.15))
})

test_that("multi-column left difference join matches rows across multiple keys", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.0, 4.0), y = c(10.0, 20.0, 30.0, 100.0))
  df2 <- data.frame(x = c(1.05, 2.1, 2.95, 3.95), y = c(10.1, 19.9, 30.05, 1.0))

  result <- fozzie_difference_left_join(df1, df2, by = c(x = "x", y = "y"), max_distance = 0.15)
  expect_equal(nrow(result), 4)
  expect_true(all(abs(result$x.x[1:3] - result$x.y[1:3]) <= 0.15))
  expect_true(all(abs(result$y.x[1:3] - result$y.y[1:3]) <= 0.15))
})

test_that("inner join skips rows with NA values", {
  df1 <- data.frame(x = c(1.0, NA, 3.0))
  df2 <- data.frame(x = c(1.05, 2.0, NA))

  result <- fozzie_difference_inner_join(df1, df2, by = c("x"), max_distance = 0.1)

  # Only (1.0, 1.05) and (3.0, NA) are possible, but NA should be skipped
  expect_equal(nrow(result), 1)
  expect_equal(result$x.x, 1.0)
  expect_equal(result$x.y, 1.05)
})
