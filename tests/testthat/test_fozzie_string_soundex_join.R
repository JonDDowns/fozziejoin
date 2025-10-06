library(testthat)

test_that("Soundex matches phonetically similar names", {
  df1 <- data.frame(name = c("Smith", "Smyth", "Ashcraft", "Tymczak"), stringsAsFactors = FALSE)
  df2 <- data.frame(name = c("Smythe", "Ashcroft", "Tymczak", "Smith"), stringsAsFactors = FALSE)

  result <- fozzie_string_join(df1, df2, by = "name", method = "soundex")

  expect_true("Smith" %in% result$name.x)
  expect_true("Smyth" %in% result$name.x)
  expect_true("Ashcraft" %in% result$name.x)
  expect_true("Tymczak" %in% result$name.x)
})

test_that("Soundex excludes non-matching names", {
  df1 <- data.frame(name = c("Smith", "Jones"), stringsAsFactors = FALSE)
  df2 <- data.frame(name = c("Taylor", "Brown"), stringsAsFactors = FALSE)

  result <- fozzie_string_join(df1, df2, by = "name", method = "soundex")

  expect_equal(nrow(result), 0)
})

test_that("Soundex handles compound names with prefixes", {
  df1 <- data.frame(name = c("VanDeusen", "De La Cruz"), stringsAsFactors = FALSE)
  df2 <- data.frame(name = c("Deusen", "Cruz"), stringsAsFactors = FALSE)

  result <- fozzie_string_join(df1, df2, by = "name", method = "soundex")

  expect_true("VanDeusen" %in% result$name.x)
  expect_true("De La Cruz" %in% result$name.x)
})

test_that("Soundex join handles NA values gracefully", {
  df1 <- data.frame(name = c("Smith", NA, "Ashcraft"), stringsAsFactors = FALSE)
  df2 <- data.frame(name = c("Smyth", "Ashcroft", NA), stringsAsFactors = FALSE)

  result <- fozzie_string_join(df1, df2, by = "name", method = "soundex")

  expect_false(any(is.na(result$name.x)))
  expect_false(any(is.na(result$name.y)))
})

test_that("Soundex join returns expected columns", {
  df1 <- data.frame(name = c("Smith"), stringsAsFactors = FALSE)
  df2 <- data.frame(name = c("Smyth"), stringsAsFactors = FALSE)

  result <- fozzie_string_join(df1, df2, by = "name", method = "soundex")

  expect_equal(sort(names(result)), sort(c("name.x", "name.y")))
})
