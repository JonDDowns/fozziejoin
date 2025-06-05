context('fozzie_inner_join')

baby_names <- data.frame(
  Name = c(
    'Liam',
    'Noah',
    'Oliver',
    'Theodore',
    'James',
    'Henry',
    'Mateo',
    'Elijah',
    'Lucas',
    'William',
    'Olivia',
    'Emma',
    'Amelia',
    'Charlotte',
    'Mia',
    'Sophia',
    'Isabella',
    'Evelyn',
    'Ava',
    'Sofia'
  ),
  rnk = rep(1:10, 2),
  year = rep(2024, 20)
)

whoops <- data.frame(
  Name = c(
    'Laim',
    'No, ahhh',
    'Olive',
    'Jams',
    'He onery',
    'A-A-ron',
    'Luças',
    'Oliv HEE-YAH',
    NA,
    'Emma',
    'Smelia',
    'Isabellü',
    'Even',
    'Ada'
  )
)


testthat::test_that('Inner join is correct for Hamming', {
  expected <- data.frame(list(
    Name.x = c('Lucas', 'Emma', 'Amelia', 'Isabella', 'Ava'),
    rnk.x = c(9L, 2L, 3L, 7L, 9L),
    year.x = c(2024, 2024, 2024, 2024, 2024),
    Name.y = c('Luças', 'Emma', 'Smelia', 'Isabellü', 'Ada')
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'hamming',
    max_distance=1,
  )

  if(!isTRUE(all.equal(actual, expected))) {
    print(actual)
  }

  testthat::expect_true(all.equal(actual, expected))

})

testthat::test_that('Inner join is correct for Levenshtein', {
  expected <- data.frame(list(
    Name.x = c('Oliver', 'James', 'Lucas', 'Emma', 'Amelia', 'Isabella', 'Ava'),
    rnk.x = c(3L, 5L, 9L, 2L, 3L, 7L, 9L),
    year.x = c(2024, 2024, 2024, 2024, 2024, 2024, 2024),
    Name.y = c('Olive', 'Jams', 'Luças', 'Emma', 'Smelia', 'Isabellü', 'Ada')
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'lv',
    max_distance=1,
  )

  if(!isTRUE(all.equal(actual, expected))) {
    print(actual)
  }

  testthat::expect_true(all.equal(actual, expected))

})


testthat::test_that('Inner join is correct for Cosine', {
  expected <- data.frame(list(
    Name.x = c(
      "Oliver", "Oliver", "James", "William", "Olivia", "Olivia", "Emma",
      "Amelia", "Isabella", "Evelyn"
    ),
    rnk.x = c(3L, 3L, 5L, 10L, 1L, 1L, 2L, 3L, 7L, 8L),
    year.x = c(2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024), 
    Name.y = c("Olive", "Oliv HEE-YAH", "Jams", "Smelia", "Olive",
      "Oliv HEE-YAH", "Emma", "Smelia", "Isabellü", "Even"
    )
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'cosine',
    max_distance=0.9,
    q=3
  )

  if(!isTRUE(all.equal(actual, expected))) {
    print(actual)
  }

  testthat::expect_true(all.equal(actual, expected))
})

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
