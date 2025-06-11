context('fozzie_right_join')

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
		'Mia',
		NA
	),
	int_col = c(1, 2, 3, 4, 5, 6, NA, 8, 9, 10, 11),
	real_col = c(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, NA, 9.0, 10.0, 11.0),
	logical_col = c(TRUE, TRUE, TRUE, TRUE, NA, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE)
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

testthat::test_that('Right join is correct for Hamming', {
	expected <- data.frame(list(
		Name.x = c("Emma", "Amelia", NA, NA, NA, NA, NA, NA, NA, NA, NA),
		int_col.x = c(NA, 8, NA, NA, NA, NA, NA, NA, NA, NA, NA),
		real_col.x = c(7, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA),
		logical_col.x = c(TRUE, FALSE, NA, NA, NA, NA, NA, NA, NA, NA, NA),
		Name.y = c(
			"Emma", "Smelia", "Laim", "No, ahhh", "Olive", "Jams", "A-A-ron",
			"Luças", "Oliv HEE-YAH", NA, "Ada"
		)
	))

	actual <- fozzie_join(
		baby_names,
		whoops,
		by = list('Name' = 'Name'),
		method = 'hamming',
		max_distance=1,
		how='right'
	)

	testthat::expect_true(all.equal(actual, expected))

	expected <- data.frame(list(
		Name.x = c("Emma", "Smelia", NA, NA, NA, NA, NA, NA, NA, NA, NA),
		Name.y = c(
			"Emma", "Amelia", "Liam", "Noah", "Oliver", "Theodore", "James", "Olivia", "Charlotte", "Mia", NA),
		int_col.y = c(NA, 8, 1, 2, 3, 4, 5, 6, 9, 10, 11), 
		real_col.y = c(7, NA, 1, 2, 3, 4, 5, 6, 9, 10, 11),
		logical_col.y = c(TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, NA, TRUE, FALSE, FALSE, FALSE)
	))
	actual <- fozzie_join(
		whoops,
		baby_names,
		by = list('Name' = 'Name'),
		method = 'hamming',
		max_distance=1,
		how='right'
	)

	testthat::expect_true(all.equal(actual, expected))
})
