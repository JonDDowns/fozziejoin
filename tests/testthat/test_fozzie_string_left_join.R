testthat::test_that("Basic left join with Hamming distance works", {
  left <- data.frame(
    Name = c("Alice", "Bob"),
    Score = c(90, 85)
  )

  right <- data.frame(
    Name = c("Alicia", "Rob", "Charlie")
  )

  expected <- data.frame(
    Name.x = c("Bob", "Alice"),
    Score = c(85, 90),
    Name.y = c("Rob", NA)
  )

  actual <- fozzie_string_join(
    left, right,
    by = list("Name" = "Name"),
    method = "hamming",
    max_distance = 2,
    how = "left",
    nthread = 1
  )

  testthat::expect_equal(actual, expected)
})

testthat::test_that("Left multi-column joins work across methods", {
  left <- data.frame(
    Name = c("Oliver", "James", "Emma", "Amelia"),
    Pet = c("Sparky", "Spike", "Fido", "Bingo")
  )
  right <- data.frame(
    Name = c("Olive", "Jams", "Emma", "Smelia"),
    Pet = c("Sparky", "Spike", "Fuselage", "Bongo")
  )

  test_cases <- list(
    lv = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", "Emma"),
        Pet.x = c("Sparky", "Spike", "Bingo", "Fido"),
        Name.y = c("Olive", "Jams", "Smelia", NA),
        Pet.y = c("Sparky", "Spike", "Bongo", NA),
        mydist_Name_Name = c(1, 1, 1, NaN),
        mydist_Pet_Pet = c(0, 0, 1, NaN)
      ),
      max_distance = 1
    ),
    hamming = list(
      expected = data.frame(
        Name.x = c("Amelia", "Oliver", "James", "Emma"),
        Pet.x = c("Bingo", "Sparky", "Spike", "Fido"),
        Name.y = c("Smelia", NA, NA, NA),
        Pet.y = c("Bongo", NA, NA, NA),
        mydist_Name_Name = c(1, NaN, NaN, NaN),
        mydist_Pet_Pet = c(1, NaN, NaN, NaN)
      ),
      max_distance = 1
    ),
    osa = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", "Emma"),
        Pet.x = c("Sparky", "Spike", "Bingo", "Fido"),
        Name.y = c("Olive", "Jams", "Smelia", NA),
        Pet.y = c("Sparky", "Spike", "Bongo", NA),
        mydist_Name_Name = c(1, 1, 1, NaN),
        mydist_Pet_Pet = c(0, 0, 1, NaN)
      ),
      max_distance = 1
    ),
    dl = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", "Emma"),
        Pet.x = c("Sparky", "Spike", "Bingo", "Fido"),
        Name.y = c("Olive", "Jams", "Smelia", NA),
        Pet.y = c("Sparky", "Spike", "Bongo", NA),
        mydist_Name_Name = c(1, 1, 1, NaN),
        mydist_Pet_Pet = c(0, 0, 1, NaN)
      ),
      max_distance = 1
    ),
    cosine = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", "Emma"),
        Pet.x = c("Sparky", "Spike", "Bingo", "Fido"),
        Name.y = c("Olive", "Jams", "Smelia", NA),
        Pet.y = c("Sparky", "Spike", "Bongo", NA),
        mydist_Name_Name = c(0.105572809000084, 0.422649730810374, 0.2, NaN),
        mydist_Pet_Pet = c(0, 0, 0.5, NaN)
      ),
      max_distance = 0.9,
      q = 2
    ),
    jw = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", "Emma"),
        Pet.x = c("Sparky", "Spike", "Bingo", "Fido"),
        Name.y = c("Olive", "Jams", "Smelia", NA),
        Pet.y = c("Sparky", "Spike", "Bongo", NA),
        mydist_Name_Name = c(0.0555555555555555, 0.0666666666666668, 0.111111111111111, NaN),
        mydist_Pet_Pet = c(0, 0, 0.133333333333333, NaN)
      ),
      max_distance = 0.5
    )
  )
  for (method in names(test_cases)) {
    case <- test_cases[[method]]
    actual <- fozzie_string_join(
      left, right,
      by = list("Name" = "Name", "Pet" = "Pet"),
      method = method,
      how = "left",
      max_distance = case$max_distance,
      distance_col = "mydist",
      q = case$q %||% NULL,
      nthread = 2
    )
    testthat::expect_equal(actual, case$expected, info = paste("Method:", method))
  }
})
