# Inner joins prove the string distance and match selection processes are correct
# For left and right joins, we only need prove that the correct non-match records
# are also included. One join test should suffice.

start_date <- as.Date("2023-01-01")
end_date <- as.Date("2023-12-31")

dates <- seq(from = start_date, to = end_date, length.out = 11)

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
	logical_col = c(TRUE, TRUE, TRUE, TRUE, NA, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE),
	date_col = dates,
	factor_col = factor(c(
		"West", "East", "West", "East", "West",
		"Midwest", "Midwest", "South", "South", "South", "South"
	))
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
		Name.x = c("Emma", "Amelia", "Liam", "Noah", "Oliver", "Theodore", "James", "Olivia", "Charlotte", "Mia", NA), 
		int_col.x = c(NA, 8, 1, 2, 3, 4, 5, 6, 9, 10, 11),
		real_col.x = c(7, NA, 1, 2, 3, 4, 5, 6, 9, 10, 11),
		logical_col.x = c(TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, NA, TRUE, FALSE, FALSE, FALSE),
		date_col.x = structure(
			c(19576.4, 19612.8, 19358, 19394.4, 19430.8, 19467.2, 19503.6, 19540, 19649.2, 19685.6, 19722),
			class = "Date"
		),
		factor_col.x = structure(
			c(2L, 3L, 4L, 1L, 4L, 1L, 4L, 2L, 3L, 3L, 3L),
			class = "factor",
			levels = c("East", "Midwest", "South", "West")
		),
		Name.y = c("Emma", "Smelia", NA, NA, NA, NA, NA, NA, NA, NA, NA)
	))

	actual <- fozzie_join(
		baby_names,
		whoops,
		by = list('Name' = 'Name'),
		method = 'hamming',
		max_distance=1,
		how='left',
		nthread=2
	)

	testthat::expect_true(all.equal(actual, expected))

})


# Levensthein
testthat::test_that('Left multi column joins work', {

	left <- data.frame(
		Name = c("Oliver", "James", "Emma", "Amelia"),
		Pet = c("Sparky", "Spike", "Fido", "Bingo")
	)
	right <- data.frame(
		Name = c("Olive", "Jams", "Emma", "Smelia"),
		Pet = c("Sparky", "Spike", "Fuselage", "Bongo")
	)

	expected <- data.frame(list(
		Name.x = c("Oliver", "James", "Amelia", "Emma"), 
		Pet.x = c("Sparky", "Spike", "Bingo", "Fido"),
		Name.y = c("Olive", "Jams", "Smelia", NA),
		Pet.y = c("Sparky", "Spike", "Bongo", NA),
		mydist_Name_Name = c(1, 1, 1, NA),
		mydist_Pet_Pet = c(0, 0, 1, NA)
	))

	actual <- fozzie_join(
		left,
		right,
		by = list('Name' = 'Name', "Pet" = "Pet"),
		method = 'lv',
		how='left',
		max_distance=1,
		distance_col="mydist",
		nthread=2
	)

	testthat::expect_true(all.equal(actual, expected))
})

