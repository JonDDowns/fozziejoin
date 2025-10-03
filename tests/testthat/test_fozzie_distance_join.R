library(testthat)
library(fozziejoin)

# Input data frames
df1 <- data.frame(
  x = c(53.55, 52.97, 67.76),
  y = c(26.78, 26.68, 57.46),
  z = c(55.06, 92.42, 32.63)
)

df2 <- data.frame(
  x = c(53.38, 53.14, 67.68),
  y = c(29.66, 26.57, 57.96),
  z = c(54.55, 92.74, 32.65)
)

all <- merge(df1, df2, by = NULL)

expected_manhattan <- all
expected_manhattan$dist <- mapply(function(i, j) {
  sum(abs(df1[i, ] - df2[j, ]))
}, rep(1:3, each = 3), rep(1:3, times = 3))
expected_manhattan <- expected_manhattan[expected_manhattan$dist <= 1, ]
rownames(expected_manhattan) <- NULL

expected_euclidean <- all
expected_euclidean$dist <- mapply(function(i, j) {
  sqrt(sum((df1[i, ] - df2[j, ])^2))
}, rep(1:3, each = 3), rep(1:3, times = 3))
expected_euclidean <- expected_euclidean[expected_euclidean$dist <= 1, ]
rownames(expected_euclidean) <- NULL


# Run fozzie join with large threshold to get all matches
result_manhattan <- fozzie_distance_inner_join(
  df1, df2,
  by = c("x", "y", "z"),
  max_distance = 1,
  method = "manhattan",
  distance_col = "dist"
)

result_euclidean <- fozzie_distance_inner_join(
  df1, df2,
  by = c("x", "y", "z"),
  max_distance = 1,
  method = "euclidean",
  distance_col = "dist"
)

test_that("Manhattan distances match R calculation", {
  expect_equal(result_manhattan, expected_manhattan, tolerance = 1e-8)
})

test_that("Euclidean distances match R calculation", {
  expect_equal(result_euclidean, expected_euclidean, tolerance = 1e-8)
})
