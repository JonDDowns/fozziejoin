baby_names <- data.frame(
	Name = c('Liam', 'Noah', 'Oliver'),
	int_col = c(1, 2, 3),
	real_col = c(1.0, 2.0, 3.0),
	logical_col = c(TRUE, TRUE, TRUE)
)

whoops <- data.frame(Name = c('Laim', 'Noahhh', 'Olive', NA))

# Levensthein
testthat::test_that('Full join is correct for Levenshtein', {
	expected <- data.frame(list(
		Name.x = c(
			"Liam", "Liam", "Liam", "Liam",
			"Noah", "Noah", "Noah", "Noah",
			"Oliver", "Oliver", "Oliver", "Oliver"
		),
		int_col.x = c(1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3),
		real_col.x = c(1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3),
		logical_col.x = c(TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE),
		Name.y = c(
			"Laim", "Noahhh", "Olive", NA,
			"Laim", "Noahhh", "Olive", NA,
			"Laim", "Noahhh", "Olive", NA
		)
	))

	actual <- fozzie_string_join(
		baby_names,
		whoops,
		by = list('Name' = 'Name'),
		method = 'lv',
		how='full',
		nthread=2
	)

	testthat::expect_true(all.equal(actual, expected))
})

# Cosine
testthat::test_that('Full join is correct for Cosine', {
	expected <- data.frame(list(
		Name.x = c(
			"Liam", "Liam", "Liam", "Liam",
			"Noah", "Noah", "Noah", "Noah",
			"Oliver", "Oliver", "Oliver", "Oliver"
		),
		int_col.x = c(1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3),
		real_col.x = c(1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3),
		logical_col.x = c(TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE),
		Name.y = c(
			"Laim", "Noahhh", "Olive", NA,
			"Laim", "Noahhh", "Olive", NA,
			"Laim", "Noahhh", "Olive", NA
		)
	))


	actual <- fozzie_string_join(
		baby_names,
		whoops,
		by = list('Name' = 'Name'),
		method = 'cosine',
		how='full',
		q=2,
		nthread=2
	)

	testthat::expect_true(all.equal(actual, expected))
})

# Jaro-Winkler
testthat::test_that('Full join is correct for JW', {
	expected <- data.frame(list(
		Name.x = c(
			"Liam", "Liam", "Liam", "Liam",
			"Noah", "Noah", "Noah", "Noah",
			"Oliver", "Oliver", "Oliver", "Oliver"
		),
		int_col.x = c(1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3),
		real_col.x = c(1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3),
		logical_col.x = c(TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE),
		Name.y = c(
			"Laim", "Noahhh", "Olive", NA,
			"Laim", "Noahhh", "Olive", NA,
			"Laim", "Noahhh", "Olive", NA
		)
	))


	actual <- fozzie_string_join(
		baby_names,
		whoops,
		by = list('Name' = 'Name'),
		method = 'jw',
		how='full',
		nthread=2
	)

	testthat::expect_true(all.equal(actual, expected))
})


