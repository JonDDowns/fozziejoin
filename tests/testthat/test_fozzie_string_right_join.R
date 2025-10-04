testthat::test_that("Basic right join with Hamming distance works", {
  left <- data.frame(
    Name = c("Alice", "Bob"),
    Score = c(90, 85)
  )

  right <- data.frame(
    Name = c("Alicia", "Rob", "Charlie")
  )

  expected <- data.frame(
    Name.x = c("Bob", NA, NA),
    Score = c(85, NA, NA),
    Name.y = c("Rob", "Alicia", "Charlie")
  )

  actual <- fozzie_string_join(
    left, right,
    by = list("Name" = "Name"),
    method = "hamming",
    max_distance = 2,
    how = "right",
    nthread = 1
  )

  testthat::expect_equal(actual, expected)
})

testthat::test_that("Right multi-column joins work across methods", {
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
        Name.x = c("Oliver", "James", "Amelia", NA),
        Pet.x = c("Sparky", "Spike", "Bingo", NA),
        Name.y = c("Olive", "Jams", "Smelia", "Emma"),
        Pet.y = c("Sparky", "Spike", "Bongo", "Fuselage"),
        mydist_Name_Name = c(1, 1, 1, NA),
        mydist_Pet_Pet = c(0, 0, 1, NA)
      ),
      max_distance = 1
    ),
    hamming = list(
      expected = data.frame(
        Name.x = c("Amelia", NA, NA, NA),
        Pet.x = c("Bingo", NA, NA, NA),
        Name.y = c("Smelia", "Olive", "Jams", "Emma"),
        Pet.y = c("Bongo", "Sparky", "Spike", "Fuselage"),
        mydist_Name_Name = c(1, NA, NA, NA),
        mydist_Pet_Pet = c(1, NA, NA, NA)
      ),
      max_distance = 1
    ),
    lcs = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", NA, NA),
        Pet.x = c("Sparky", "Spike", NA, NA),
        Name.y = c("Olive", "Jams", "Emma", "Smelia"),
        Pet.y = c("Sparky", "Spike", "Fuselage", "Bongo"),
        mydist_Name_Name = c(1, 1, NaN, NaN),
        mydist_Pet_Pet = c(0, 0, NaN, NaN)
      ),
      max_distance = 1
    ),
    osa = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", NA),
        Pet.x = c("Sparky", "Spike", "Bingo", NA),
        Name.y = c("Olive", "Jams", "Smelia", "Emma"),
        Pet.y = c("Sparky", "Spike", "Bongo", "Fuselage"),
        mydist_Name_Name = c(1, 1, 1, NA),
        mydist_Pet_Pet = c(0, 0, 1, NA)
      ),
      max_distance = 1
    ),
    dl = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", NA),
        Pet.x = c("Sparky", "Spike", "Bingo", NA),
        Name.y = c("Olive", "Jams", "Smelia", "Emma"),
        Pet.y = c("Sparky", "Spike", "Bongo", "Fuselage"),
        mydist_Name_Name = c(1, 1, 1, NA),
        mydist_Pet_Pet = c(0, 0, 1, NA)
      ),
      max_distance = 1
    ),
    cosine = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", NA),
        Pet.x = c("Sparky", "Spike", "Bingo", NA),
        Name.y = c("Olive", "Jams", "Smelia", "Emma"),
        Pet.y = c("Sparky", "Spike", "Bongo", "Fuselage"),
        mydist_Name_Name = c(0.105572809000084, 0.422649730810374, 0.2, NaN),
        mydist_Pet_Pet = c(0, 0, 0.5, NaN)
      ),
      max_distance = 0.9,
      q = 2
    ),
    qgram = list(
      expected = data.frame(
        Name.x = c("Oliver", rep(NA, 3)),
        Pet.x = c("Sparky", rep(NA, 3)),
        Name.y = c("Olive", "Jams", "Emma", "Smelia"),
        Pet.y = c("Sparky", "Spike", "Fuselage", "Bongo"),
        mydist_Name_Name = c(1, rep(NaN, 3)),
        mydist_Pet_Pet = c(0, rep(NaN, 3))
      ),
      max_distance = 1,
      q = 2
    ),
    jaccard = list(
      expected = data.frame(
        Name.x = c("Oliver", rep(NA, 3)),
        Pet.x = c("Sparky", rep(NA, 3)),
        Name.y = c("Olive", "Jams", "Emma", "Smelia"),
        Pet.y = c("Sparky", "Spike", "Fuselage", "Bongo"),
        mydist_Name_Name = c(0.2, rep(NaN, 3)),
        mydist_Pet_Pet = c(0, rep(NaN, 3))
      ),
      max_distance = 0.5,
      q = 2
    ),
    jw = list(
      expected = data.frame(
        Name.x = c("Oliver", "James", "Amelia", NA),
        Pet.x = c("Sparky", "Spike", "Bingo", NA),
        Name.y = c("Olive", "Jams", "Smelia", "Emma"),
        Pet.y = c("Sparky", "Spike", "Bongo", "Fuselage"),
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
      how = "right",
      max_distance = case$max_distance,
      distance_col = "mydist",
      q = case$q %||% NULL,
      nthread = 2
    )
    testthat::expect_equal(actual, case$expected)
  }
})
