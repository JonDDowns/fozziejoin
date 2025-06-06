context('fozzie_left_join')

# Inner joins prove the string distance and match selection processes are correct
# For left and right joins, we only need prove that the correct non-match records
# are also included. One join test should suffice.

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

testthat::test_that('Left join is correct for Hamming', {
  expected <- data.frame(list(
  Name.x = c(
    "Emma", "Amelia", "Liam", "Noah", "Oliver", 
    "Theodore", "James", "Olivia", "Charlotte", "Mia"
  ),
  int_col.x = c(NA, 8, 1, 2, 3, 4, 5, 6, 9, 10),
  real_col.x = c(7, NA, 1, 2, 3, 4, 5, 6, 9, 10), 
  logical_col.x = c(TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, NA, TRUE, FALSE, FALSE), 
  Name.y = c("Emma", "Smelia", NA, NA, NA, NA, NA, NA, NA, NA)
  ))

  actual <- fozzie_join(
    baby_names,
    whoops,
    by = list('Name' = 'Name'),
    method = 'hamming',
    max_distance=1,
    how='left'
  )

  testthat::expect_true(all.equal(actual, expected))

})
