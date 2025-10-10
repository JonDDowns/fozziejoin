df1 <- data.frame(x1 = 1:3, x2 = letters[1:3], shared = c("a", "b", "c"))
df2 <- data.frame(x2 = 4:6, x1 = LETTERS[1:3], shared = c("a", "b", "d"))

test_that("auto-detects shared columns when by is NULL", {
  suppressMessages(result <- normalize_by(df1, df2, NULL))
  expect_equal(result, list(x1 = "x1", x2 = "x2", shared = "shared"))
})

test_that("handles character vector of length 1", {
  result <- normalize_by(df1, df2, "x1")
  expect_equal(result, list(x1 = "x1"))
})

test_that("handles longer character vectors", {
  result <- normalize_by(df1, df2, c("x1", "x2"))
  expect_equal(result, list(x1 = "x1", x2 = "x2"))
})

test_that("handles named character vector", {
  result <- normalize_by(df1, df2, c("x1" = "x2"))
  expect_equal(result, list(x1 = "x2"))
})

test_that("handles character vector of length 2", {
  result <- normalize_by(df1, df2, c("x1", "x2"))
  expect_equal(result, list(x1 = "x1", x2 = "x2"))
})

test_that("fills in missing names in partially named list", {
  result <- normalize_by(df1, df2, list("x2"))
  expect_equal(result, list(x2 = "x2"))
})

test_that("throws error when no shared columns", {
  df3 <- data.frame(a = 1:3)
  df4 <- data.frame(b = 4:6)
  expect_error(normalize_by(df3, df4, NULL), "No shared column names")
})

test_that("throws error on invalid character vector length", {
  expect_error(normalize_by(df1, df2, c("x1", "x2", "x3")), "The following columns")
})
