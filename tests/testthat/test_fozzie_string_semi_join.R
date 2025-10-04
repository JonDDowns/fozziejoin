whoops <- data.frame(
  Name = c(
    "Laim",
    "No, ahhh",
    "Olive",
    "Jams",
    "A-A-ron",
    "Luças",
    "Oliv HEE-YAH",
    "Emma",
    "Smelia",
    NA,
    "Ada"
  )
)

# Levensthein
testthat::test_that("Semi join is correct for Levenshtein", {
  expected <- test_df[-c(1:2, 4, 6, 9, 10), ]
  rownames(expected) <- NULL
  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "lv",
    how = "semi",
    max_distance = 1,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))
})
