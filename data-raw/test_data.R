test_df <- data.frame(list(
	Name = c(
		"Liam", "Noah", "Oliver", "Theodore", 
		"James", "Olivia", "Emma", "Amelia", "Charlotte", "Mia"
	),
	int_col = c(1, 2, 3, 4, 5, 6, NA, 8, 9, 10),
	real_col = c(1, 2, 3, 4, 5, 6, 7, NA, 9, 10),
	logical_col = c(TRUE, TRUE, TRUE, TRUE, NA, TRUE, TRUE, FALSE, FALSE, FALSE),
	date_col = structure(c(18262, 18263, 18264, 18265, 18266, 18267, 18268, 18269, 18270, 18271), class = "Date"), 
	posixct_col = structure(
		c(1577908800, 1577912400, 1577916000, 1577919600, 1577923200, 1577926800, 1577930400, 1577934000, 1577937600, 1577941200),
		class = c("POSIXct", "POSIXt")
	), 
	posixlt_col = structure(
		c(1577908800, 1577912400, 1577916000, 1577919600, 1577923200, 1577926800, 1577930400, 1577934000, 1577937600, 1577941200),
		class = c("POSIXct", "POSIXt")
	),
	factor_col = structure(
		c(1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L, 5L, 5L),
		levels = c("A", "B", "C", "D", "E"), class = "factor")
))
usethis::use_data(test_df, overwrite=TRUE)
