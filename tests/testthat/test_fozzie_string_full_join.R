baby_names <- data.frame(
  Name = c("Liam", "Noah", "Oliver"),
  int_col = c(1, 2, 3),
  real_col = c(1.0, 2.0, 3.0),
  logical_col = c(TRUE, TRUE, TRUE)
)

whoops <- data.frame(Name = c("Laim", "Noahhh", "Olive", NA))

# Levensthein
testthat::test_that("Full join is correct for Levenshtein", {
  expected <- data.frame(list(
    Name.x = c("Oliver", "Liam", "Noah", NA, NA, NA),
    int_col = c(3, 1, 2, NA, NA, NA),
    real_col = c(3, 1, 2, NA, NA, NA),
    logical_col = c(TRUE, TRUE, TRUE, NA, NA, NA),
    Name.y = c("Olive", NA, NA, "Laim", "Noahhh", NA)
  ))

  actual <- fozzie_string_join(
    baby_names,
    whoops,
    by = list("Name" = "Name"),
    method = "lv",
    max_distance = 1,
    how = "full",
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))
})

# Cosine
testthat::test_that("Full join is correct for Cosine", {
  expected <- data.frame(list(
    Name.x = c("Noah", "Oliver", "Liam", NA, NA),
    int_col = c(2, 3, 1, NA, NA),
    real_col = c(2, 3, 1, NA, NA),
    logical_col = c(TRUE, TRUE, TRUE, NA, NA),
    Name.y = c("Noahhh", "Olive", NA, "Laim", NA)
  ))

  actual <- fozzie_string_join(
    baby_names,
    whoops,
    by = list("Name" = "Name"),
    method = "cosine",
    how = "full",
    max_distance = 0.5,
    q = 2,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))
})

# Jaro-Winkler
testthat::test_that("Full join is correct for JW", {
  expected <- data.frame(list(
    Name.x = c("Liam", "Noah", "Noah", "Oliver", NA),
    int_col = c(1, 2, 2, 3, NA),
    real_col = c(1, 2, 2, 3, NA),
    logical_col = c(TRUE, TRUE, TRUE, TRUE, NA),
    Name.y = c("Laim", "Laim", "Noahhh", "Olive", NA)
  ))

  actual <- fozzie_string_join(
    baby_names,
    whoops,
    by = list("Name" = "Name"),
    method = "jw",
    how = "full",
    max_distance = 0.5,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))
})
