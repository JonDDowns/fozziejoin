test_that("inner join returns matched rows within threshold", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.0))
  df2 <- data.frame(x = c(1.05, 2.1, 2.95))

  result <- fozzie_difference_inner_join(df1, df2, by = c("x", "x"), max_distance = 0.1)
  expect_equal(nrow(result), 3)
})

test_that("left join includes all rows from df1", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.0))
  df2 <- data.frame(x = c(1.05, 2.1))

  result <- fozzie_difference_left_join(df1, df2, by = c("x", "x"), max_distance = 0.05)
  expect_equal(nrow(result), 3)
  expect_true(any(is.na(result$x.y)))
})

test_that("right join includes all rows from df2", {
  df1 <- data.frame(x = c(1.0, 2.0))
  df2 <- data.frame(x = c(1.05, 2.1, 3.0))

  result <- fozzie_difference_right_join(df1, df2, by = c("x", "x"), max_distance = 0.05)
  expect_equal(nrow(result), 3)
  expect_true(any(is.na(result$x.x)))
})

test_that("anti join returns unmatched rows from df1", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.0))
  df2 <- data.frame(x = c(1.05, 2.1))

  result <- fozzie_difference_anti_join(df1, df2, by = c("x", "x"), max_distance = 0.05)
  expect_equal(nrow(result), 2)
  expect_equal(result$x, c(2.0, 3.0))
})

test_that("full join includes all rows from both tables", {
  df1 <- data.frame(x = c(1.0, 2.0, 3.1))
  df2 <- data.frame(x = c(2.1, 3.0, 4.0))

  result <- fozzie_difference_full_join(df1, df2, by = c("x", "x"), max_distance = 0.05)
  expect_equal(nrow(result), 6)
})

test_that("distance_col is correctly computed", {
  df1 <- data.frame(x = c(1.0))
  df2 <- data.frame(x = c(1.05))

  result <- fozzie_difference_inner_join(df1, df2, by = c("x", "x"), max_distance = 0.1, distance_col = "diff")
  expect_true("diff" %in% names(result))
  expect_equal(result$diff, 0.05)
})

test_that("named list for `by` works", {
  df1 <- data.frame(a = c(1.0))
  df2 <- data.frame(b = c(1.05))

  result <- fozzie_difference_inner_join(df1, df2, by = list(a = "b"), max_distance = 0.1)
  expect_equal(nrow(result), 1)
})
