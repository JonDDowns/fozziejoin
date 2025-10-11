test_that("regex join matches correctly using right-hand regex patterns", {
  df1 <- data.frame(name = c("apple", "banana", "cherry", "melon"))
  df2 <- data.frame(pattern = c("^a", "an", "rry$"))

  inner <- fozzie_regex_inner_join(df1, df2, by = c("name" = "pattern"))
  left <- fozzie_regex_left_join(df1, df2, by = c("name" = "pattern"))

  # Check expected matches in inner join
  expect_equal(inner$name, c("apple", "banana", "cherry"))
  expect_equal(inner$pattern, c("^a", "an", "rry$"))

  # Check left join preserves all rows from df1
  expect_equal(left$name, df1$name)
  expect_equal(sum(!is.na(left$pattern)), 3)  # 3 matches, 1 NA
})

test_that("regex join respects ignore_case = TRUE and FALSE", {
  df1 <- data.frame(name = c("Apple", "Banana", "Cherry", "Melon"))
  df2 <- data.frame(pattern = c("^a", "an", "rry$"))

  # Case-sensitive: no matches
  inner_sensitive <- fozzie_regex_inner_join(df1, df2, by = c("name" = "pattern"), ignore_case = FALSE)
  expect_equal(nrow(inner_sensitive), 2)

  # Case-insensitive: all should match
  inner_insensitive <- fozzie_regex_inner_join(df1, df2, by = c("name" = "pattern"), ignore_case = TRUE)
  expect_equal(inner_insensitive$name, c("Apple", "Banana", "Cherry"))
})

test_that("regex join allows multiple matches per value", {
  df1 <- data.frame(name = c("apple"))
  df2 <- data.frame(pattern = c("^a", "pp", "le$"))

  result <- fozzie_regex_inner_join(df1, df2, by = c("name" = "pattern"))
  expect_equal(nrow(result), 3)
  expect_equal(result$name, rep("apple", 3))
})

test_that("regex left join fills unmatched rows with NA", {
  df1 <- data.frame(name = c("kiwi", "grape"))
  df2 <- data.frame(pattern = c("^a", "banana"))

  left <- fozzie_regex_left_join(df1, df2, by = c("name" = "pattern"))
  expect_equal(nrow(left), 2)
  expect_true(all(is.na(left$pattern)))
})

test_that("regex join throws error on invalid patterns", {
  df1 <- data.frame(name = c("apple"))
  df2 <- data.frame(pattern = c("(", "^a"))

  expect_error(fozzie_regex_inner_join(df1, df2, by = c("name" = "pattern")))
})

test_that("regex join handles anchors correctly", {
  df1 <- data.frame(name = c("apple pie", "pie apple", "apple"))
  df2 <- data.frame(pattern = c("^apple", "apple$"))

  result <- fozzie_regex_inner_join(df1, df2, by = c("name" = "pattern"))
  expect_equal(nrow(result), 4)
  expect_equal(result$name, c('apple pie', 'pie apple', 'apple', 'apple'))
})


