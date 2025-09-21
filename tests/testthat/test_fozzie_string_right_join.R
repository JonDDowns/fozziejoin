# Inner joins prove the string distance and match selection processes are correct
# For left and right joins, we only need prove that the correct non-match records
# are also included. One join test should suffice.
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
		Name.x = c("Emma", "Amelia", rep(NA, 9)),
		int_col.x = c(NA, 8, rep(NA, 9)),
		real_col.x = c(7, rep(NA, 10)), 
		logical_col.x = c(TRUE, FALSE, rep(NA, 9)),
		date_col.x = structure(
			c(18268, 18269, rep(NA, 9)), class = "Date"
		),
		posixct_col.x = structure(
			c(1577930400, 1577934000, rep(NA, 9))
			, class = c("POSIXct", "POSIXt")
		),
		posixlt_col.x = structure(
			c(1577930400, 1577934000, rep(NA, 9)),
			class = c("POSIXct", "POSIXt")
		),
		factor_col.x = c(4L, 4L, rep(NA, 9)),
		Name.y = c(
			"Emma", "Smelia", "Laim", "No, ahhh", "Olive",
			"Jams", "A-A-ron", "Luças", "Oliv HEE-YAH", NA, "Ada"
		)
	))
	actual <- fozzie_string_join(
		test_df,
		whoops,
		by = list('Name' = 'Name'),
		method = 'hamming',
		max_distance=1,
		how='right',
		nthread=2
	)

	testthat::expect_true(all.equal(actual, expected))
})


# Multi columns
testthat::test_that('Right multi column joins work', {

	left <- data.frame(
		Name = c("Oliver", "James", "Emma", "Amelia"),
		Pet = c("Sparky", "Spike", "Fido", "Bingo")
	)
	right <- data.frame(
		Name = c("Olive", "Jams", "Emma", "Smelia"),
		Pet = c("Sparky", "Spike", "Fuselage", "Bongo")
	)

	expected <- data.frame(list(
		Name.x = c("Oliver", "James", "Amelia", NA), 
		Pet.x = c("Sparky", "Spike", "Bingo", NA),
		Name.y = c("Olive", "Jams", "Smelia", "Emma"),
		Pet.y = c("Sparky", "Spike", "Bongo", "Fuselage"),
		mydist_Name_Name = c(1, 1, 1, NA),
		mydist_Pet_Pet = c(0, 0, 1, NA)
	))

	actual <- fozzie_string_join(
		left,
		right,
		by = list('Name' = 'Name', "Pet" = "Pet"),
		method = 'lv',
		how='right',
		max_distance=1,
		distance_col="mydist",
		nthread=2
	)

	testthat::expect_true(all.equal(actual, expected))
})
