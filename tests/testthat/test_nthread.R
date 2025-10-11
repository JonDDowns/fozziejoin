test_that("multi-column left difference join matches rows across multiple keys", {
  df1 <- data.frame(x = rep(c(1.0, 2.0, 3.0, 4.0), 10), y = rep(c(10.0, 20.0, 30.0, 100.0), 10))
  df2 <- data.frame(x = c(1.05, 2.1, 2.95, 3.95), y = c(10.1, 19.9, 30.05, 1.0))

  # Default should be 2 in testing normally
  runtime <- system.time(fozzie_difference_left_join(
        df1, df2, by = c(x = "x", y = "y"), max_distance = 0.15
  ))
  testthat::expect_lte(runtime["user.self"], 2.6 * runtime["elapsed"])

  # We should still be able to force it to be 1 thread
  runtime <- system.time(fozzie_difference_left_join(
        df1, df2, by = c(x = "x", y = "y"), max_distance = 0.15, nthread = 1
  ))
  testthat::expect_lt(runtime["user.self"], 2 * runtime["elapsed"])
})
