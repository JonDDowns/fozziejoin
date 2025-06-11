context('fozzie_anti_join')

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
testthat::test_that('Anti join is correct for Levenshtein', {
	expected <- data.frame(list(
		Name = c("Liam", "Noah", "Theodore", "Olivia", "Charlotte", "Mia"),
		int_col = c(1, 2, 4, 6, 9, 10),
		real_col = c(1, 2, 4, 6, 9, 10),
		logical_col = c(TRUE, TRUE, TRUE, TRUE, FALSE, FALSE)
	))
	actual <- fozzie_join(
		baby_names,
		whoops,
		by = list('Name' = 'Name'),
		method = 'lv',
		how='anti',
		max_distance=1,
	)

	testthat::expect_true(all.equal(actual, expected))
})


