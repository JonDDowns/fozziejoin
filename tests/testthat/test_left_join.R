# Inner joins prove the string distance and match selection processes are correct
# For left and right joins, we only need prove that the correct non-match records
# are also included and that all attributes are preserved in output.

make_expected <- function(rows, name_y) {
	df <- test_df[rows, ]
	names(df) <- paste0(names(df), ".x")
	df$Name.y <- name_y
	rownames(df) <- NULL
	df
}

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
	expected <- make_expected(
		rows = c(7:8, 1:6, 9:10),
		name_y = c("Emma", "Smelia", rep(NA, 8))
	)

	actual <- fozzie_join(
		test_df,
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

