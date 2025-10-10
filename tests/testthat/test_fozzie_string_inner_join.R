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

make_expected <- function(rows, name_y) {
  df <- test_df[rows, ]
  colnames(df)[1] <- paste0(colnames(df)[1], ".x")
  df$Name.y <- name_y
  rownames(df) <- NULL
  df
}

# Levensthein
testthat::test_that("Inner join is correct for Levenshtein", {
  # Rewritten expected using helper
  expected <- make_expected(
    rows = c(3, 5, 7, 8),
    name_y = c("Olive", "Jams", "Emma", "Smelia")
  )
  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "lv",
    how = "inner",
    max_distance = 1,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))

  expected$mydist <- c(1, 1, 0, 1)

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "lv",
    how = "inner",
    max_distance = 1,
    distance_col = "mydist",
    nthread = 2
  )
  testthat::expect_true(all.equal(actual, expected))
})

# Damerau-Levenshtein

# Hamming
testthat::test_that("Inner join is correct for Hamming", {
  expected <- make_expected(
    rows = c(7, 8),
    name_y = c("Emma", "Smelia")
  )

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "hamming",
    max_distance = 1,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))

  expected$mydist <- c(0, 1)

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "hamming",
    how = "inner",
    max_distance = 1,
    distance_col = "mydist",
    nthread = 2
  )
  testthat::expect_true(all.equal(actual, expected))
})

# LCS
testthat::test_that("Inner join is correct for LCS", {
  expected <- make_expected(c(3, 5, 7), c("Olive", "Jams", "Emma"))

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "lcs",
    max_distance = 1,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))

  expected$mydist <- c(1, 1, 0)

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "lcs",
    how = "inner",
    max_distance = 1,
    distance_col = "mydist",
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))
})

# qgram
testthat::test_that("Inner join is correct for QGram", {
  expected <- make_expected(c(3, 7), c("Olive", "Emma"))

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "qgram",
    max_distance = 1,
    q = 2,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))

  expected$mydist <- c(1, 0)

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "qgram",
    how = "inner",
    max_distance = 1,
    q = 2,
    distance_col = "mydist",
    nthread = 2
  )
  testthat::expect_true(all.equal(actual, expected))
})

# Cosine
testthat::test_that("Inner join is correct for Cosine", {
  expected <- make_expected(
    c(3, 3, 5, 6, 6, 7, 8),
    c("Olive", "Oliv HEE-YAH", "Jams", "Olive", "Oliv HEE-YAH", "Emma", "Smelia")
  )
  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "cosine",
    max_distance = 0.9,
    q = 3,
    nthread = 2
  )
  testthat::expect_true(all.equal(actual, expected))

  expected$mydist <- c(
    0.133974596215561,
    0.683772233983162,
    0.591751709536137,
    0.422649730810374,
    0.683772233983162,
    0,
    0.25
  )
  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "cosine",
    max_distance = 0.9,
    q = 3,
    distance_col = "mydist",
    nthread = 2
  )
  testthat::expect_true(all.equal(actual, expected))
})

# Jaccard
testthat::test_that("Inner join is correct for Jaccard", {
  expected <- make_expected(
    c(3, 3, 5, 6, 6, 7, 8),
    c("Olive", "Oliv HEE-YAH", "Jams", "Olive", "Oliv HEE-YAH", "Emma", "Smelia")
  )

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "jaccard",
    max_distance = 0.9,
    q = 3,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))

  expected$mydist <- c(0.25, 5 / 6, 0.75, 0.6, 5 / 6, 0, 0.4)
  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "jaccard",
    max_distance = 0.9,
    q = 3,
    distance_col = "mydist",
    nthread = 2
  )
  testthat::expect_true(all.equal(actual, expected))
})


# Jaro-Winkler
testthat::test_that("Inner join is correct for Jaro-Winkler", {
  expected <- make_expected(
    c(1, 2, 3, 5, 6, 7, 8),
    c("Laim", "No, ahhh", "Olive", "Jams", "Olive", "Emma", "Smelia")
  )
  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "jw",
    max_distance = 0.2,
    nthread = 2,
  )

  testthat::expect_true(all.equal(actual, expected))
})

# OSA
testthat::test_that("Inner join is correct for OSA", {
  expected <- make_expected(
    c(1, 3, 5, 7, 8),
    c("Laim", "Olive", "Jams", "Emma", "Smelia")
  )

  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "osa",
    max_distance = 1,
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))

  expected$mydist <- c(1, 1, 1, 0, 1)
  actual <- fozzie_string_join(
    test_df,
    whoops,
    by = list("Name" = "Name"),
    method = "osa",
    max_distance = 1,
    distance_col = "mydist",
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))
})

testthat::test_that("Non-strings throw an error", {
  testthat::expect_error(
    fozzie_string_join(
      test_df, whoops,
      by = list("year" = "Name"), method = "hamming",
      max_distance = 1, q = 3, nthread = 2
    )
  )
})

testthat::test_that("Invalid columns throw error", {
  testthat::expect_error(
    fozzie_string_join(
      test_df, whoops,
      by = list("DoesNotExist" = "Name"), method = "hamming",
      max_distance = 1, q = 3, nthread = 2
    )
  )
})

testthat::test_that("Multi column joins work", {
  left <- data.frame(
    Name = c("Oliver", "James", "Emma", "Amelia"),
    Pet = c("Sparky", "Spike", "Fido", "Bingo")
  )
  right <- data.frame(
    Name = c("Olive", "Jams", "Emma", "Smelia"),
    Pet = c("Sparky", "Spike", "Fuselage", "Bongo")
  )

  expected <- data.frame(list(
    Name.x = c("Oliver", "James", "Amelia"),
    Pet.x = c("Sparky", "Spike", "Bingo"),
    Name.y = c("Olive", "Jams", "Smelia"),
    Pet.y = c("Sparky", "Spike", "Bongo"),
    mydist_Name_Name = c(1, 1, 1),
    mydist_Pet_Pet = c(0, 0, 1)
  ))

  actual <- fozzie_string_join(
    left,
    right,
    by = list("Name" = "Name", "Pet" = "Pet"),
    method = "lv",
    how = "inner",
    max_distance = 1,
    distance_col = "mydist",
    nthread = 2
  )

  testthat::expect_true(all.equal(actual, expected))
})

testthat::test_that("nthread argument works for unnormalized edit distances", {
  # The runtime is so small that false positives will pop up.
  # Need to artificially inflate the test DF size.

  unnorm_methods <- c("hamming", "osa", "dl", "lcs", "lv")
  for (method in unnorm_methods) {
    runtime <- system.time(fozzie_string_join(
      do.call(rbind, replicate(10, test_df, simplify = FALSE)),
      whoops,
      by = c('Name'),
      method = method,
      max_distance = 1,
      nthread = 2
    ))
    testthat::expect_lte(runtime["user.self"], 2.5 * runtime["elapsed"])
  }
})

testthat::test_that("nthread argument works for normalized edit distances", {
  # The runtime is so small that false positives will pop up.
  # Need to artificially inflate the test DF size.

  norm_methods <- c("jw")
  for (method in norm_methods) {
    runtime <- system.time(fozzie_string_join(
      do.call(rbind, replicate(10, test_df, simplify = FALSE)),
      whoops,
      by = c('Name'),
      method = method,
      max_distance = 1,
      nthread = 2
    ))
    testthat::expect_lte(runtime["user.self"], 2.5 * runtime["elapsed"])
  }
})

testthat::test_that("nthread argument works for qgram edit distances", {
  # The runtime is so small that false positives will pop up.
  # Need to artificially inflate the test DF size.

  norm_methods <- c("cosine", "jaccard", "qgram")
  for (method in norm_methods) {
    runtime <- system.time(fozzie_string_join(
      do.call(rbind, replicate(10, test_df, simplify = FALSE)),
      whoops,
      by = 'Name',
      method = method,
      max_distance = 1,
      q = 2,
      nthread = 2
    ))
    testthat::expect_lte(runtime["user.self"], 2.5 * runtime["elapsed"])
  }
})
