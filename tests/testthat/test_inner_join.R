context('fozzie_inner_join')

baby_names <- data.frame(
  Name = c(
    'Liam',
    'Noah',
    'Oliver',
    'Theodore',
    'James',
    'Olivia',
    'Emma',
    'Amelia',
    'Charlotte',
    'Mia'
  ),
  int_col = c(1, 2, 3, 4, 5, 6, NA, 8, 9, 10),
  real_col = c(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, NA, 9.0, 10.0),
  logical_col = c(TRUE, TRUE, TRUE, TRUE, NA, TRUE, TRUE, FALSE, FALSE, FALSE)
)

whoops <- data.frame(
  Name = c(
    'Laim',
    'No, ahhh',
    'Olive',
    'Jams',
    'A-A-ron',
    'Luças',
    'Oliv HEE-YAH',
    'Emma',
    'Smelia',
    NA,
    'Ada'
  )
)

# Levensthein
testthat::test_that('Inner join is correct for Levenshtein', {
  expected <- data.frame(list(
    Name.x = c("Oliver", "James", "Emma", "Amelia"),
    int_col.x = c(3, 5, NA, 8),
    real_col.x = c(3, 5, 7, NA),
    logical_col.x = c(TRUE, NA, TRUE, FALSE),
    Name.y = c("Olive", "Jams", "Emma", "Smelia")
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'lv',
    how='inner',
    max_distance=1
  )
  testthat::expect_true(all.equal(actual, expected))

})

# Damerau-Levenshtein

# Hamming
testthat::test_that('Inner join is correct for Hamming', {
  expected <- data.frame(list(
    Name.x = c("Emma", "Amelia"),
    int_col.x = c(NA, 8),
    real_col.x = c(7, NA),
    logical_col.x = c(TRUE, FALSE),
    Name.y = c("Emma", "Smelia")
  ))
  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'hamming',
    max_distance=1,
  )

  testthat::expect_true(all.equal(actual, expected))

})

# LCS
#testthat::test_that('Inner join is correct for LCS', {
#  expected <- data.frame(list(
#    Name.x = c(
#      "Oliver", "Oliver", "James", "William", "Olivia", "Olivia", "Emma",
#      "Amelia", "Isabella", "Evelyn"
#    ),
#    rnk.x = c(3L, 3L, 5L, 10L, 1L, 1L, 2L, 3L, 7L, 8L),
#    year.x = c(2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024), 
#    Name.y = c("Olive", "Oliv HEE-YAH", "Jams", "Smelia", "Olive",
#      "Oliv HEE-YAH", "Emma", "Smelia", "Isabellü", "Even"
#    )
# ))

#  actual <- fozzie_join(
#    baby_names,
#    whoops,
#    by = list('Name' = 'Name'),
#    method = 'lcs',
#    max_distance=2
#  )
#
#  hm = fuzzyjoin::stringdist_inner_join(
#    baby_names, whoops, by=c('Name' = 'Name'), method='lcs', max_dist=2, distance_col='dist'
#  )
#
#  if(!isTRUE(all.equal(actual, expected))) {
#    print(actual)
#  }
#
#  testthat::expect_true(all.equal(actual, expected))
#})

# qgram
testthat::test_that('Inner join is correct for QGram', {
  expected <- data.frame(list(
    Name.x = c("Oliver", "Emma"),
    int_col.x = c(3, NA),
    real_col.x = c(3, 7),
    logical_col.x = c(TRUE, TRUE),
    Name.y = c("Olive", "Emma"))
  )

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'qgram',
    max_distance=1,
    q=2
  )

  testthat::expect_true(all.equal(actual, expected))
})

# Cosine
testthat::test_that('Inner join is correct for Cosine', {
  expected <- data.frame(list(
    Name.x = c("Oliver", "Oliver", "James", "Olivia", "Olivia", "Emma", "Amelia"),
    int_col.x = c(3, 3, 5, 6, 6, NA, 8),
    real_col.x = c(3, 3, 5, 6, 6, 7, NA),
    logical_col.x = c(TRUE, TRUE, NA, TRUE, TRUE, TRUE, FALSE),
    Name.y = c("Olive", "Oliv HEE-YAH", "Jams", "Olive", "Oliv HEE-YAH", "Emma", "Smelia")
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'cosine',
    max_distance=0.9,
    q=3
  )
  testthat::expect_true(all.equal(actual, expected))
})

# Jaccard
testthat::test_that('Inner join is correct for Jaccard', {
  expected <- data.frame(list(
    Name.x = c("Oliver", "Oliver", "James", "Olivia", "Olivia", "Emma", "Amelia"), 
    int_col.x = c(3, 3, 5, 6, 6, NA, 8),
    real_col.x = c(3, 3, 5, 6, 6, 7, NA),
    logical_col.x = c(TRUE, TRUE, NA, TRUE, TRUE, TRUE, FALSE),
    Name.y = c("Olive", "Oliv HEE-YAH", "Jams", "Olive", "Oliv HEE-YAH", "Emma", "Smelia")
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'jaccard',
    max_distance=0.9,
    q=3
  )

  testthat::expect_true(all.equal(actual, expected))
})


# Jaro-Winkler
testthat::test_that('Inner join is correct for Jaro-Winkler', {
  expected <- data.frame(list(
    Name.x = c("Liam", "Noah", "Oliver", "Oliver", "James", "Olivia", "Olivia", "Emma", "Amelia"), 
    int_col.x = c(1, 2, 3, 3, 5, 6, 6, NA, 8),
    real_col.x = c(1, 2, 3, 3, 5, 6, 6, 7, NA),
    logical_col.x = c(TRUE, TRUE, TRUE, TRUE, NA, TRUE, TRUE, TRUE, FALSE),
    Name.y = c("Laim", "No, ahhh", "Olive", "Oliv HEE-YAH", "Jams", "Olive", "Oliv HEE-YAH", "Emma", "Smelia")
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'jw',
    max_distance=0.2
  )

  testthat::expect_true(all.equal(actual, expected))
})

# Jaro
testthat::test_that('Inner join is correct for Jaro', {
  expected <- data.frame(list(
    Name.x = c("Liam", "Noah", "Oliver", "James", "Olivia", "Emma", "Amelia"),
    int_col.x = c(1, 2, 3, 5, 6, NA, 8),
    real_col.x = c(1, 2, 3, 5, 6, 7, NA),
    logical_col.x = c(TRUE, TRUE, TRUE, NA, TRUE, TRUE, FALSE),
    Name.y = c("Laim", "No, ahhh", "Olive", "Jams", "Olive", "Emma", "Smelia")
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'jaro',
    max_distance=0.2
  )

  testthat::expect_true(all.equal(actual, expected))
})

# OSA
testthat::test_that('Inner join is correct for OSA', {
  expected <- data.frame(list(
    Name.x = c("Liam", "Oliver", "James", "Emma", "Amelia"),
    int_col.x = c(1, 3, 5, NA, 8),
    real_col.x = c(1, 3, 5, 7, NA),
    logical_col.x = c(TRUE, TRUE, NA, TRUE, FALSE), 
    Name.y = c("Laim", "Olive", "Jams", "Emma", "Smelia"))
  )

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'osa',
    max_distance=1
  )

  testthat::expect_true(all.equal(actual, expected))
})

testthat::test_that('Non-strings throw an error', {
  testthat::expect_error(
    fozzie_join(
      baby_names, whoops, by=list('year' = 'Name'), method='hamming',
      max_distance=1, q=3
    )
  )
})

testthat::test_that('Invalid columns throw error', {
  testthat::expect_error(
    fozzie_join(
      baby_names, whoops, by=list('DoesNotExist' = 'Name'), method='hamming',
      max_distance=1, q=3
    )
  )
})
