## Defaults of the options FireSense production has always set: with the option unset, the package behaves as
## if it were set to that value. An explicit setting still wins.

test_that("onBadHost drops a failing host unless the user says otherwise", {
  withr::local_options(clusters.onBadHost = NULL)
  expect_identical(clusters:::.clustersOption("onBadHost"), "drop")
  withr::local_options(clusters.onBadHost = "stop")
  expect_identical(clusters:::.clustersOption("onBadHost"), "stop")
})

test_that("waitForCores waits up to 8 hours unless the user says otherwise", {
  withr::local_options(clusters.waitForCores = NULL)
  expect_identical(clusters:::.clustersOption("waitForCores"), 8 * 3600)
  expect_identical(eval(formals(clusters:::.allocateWhenAvailable)$waitSeconds), 8 * 3600)
  expect_identical(eval(formals(reserveCapacity)$waitSeconds), 8 * 3600)
  withr::local_options(clusters.waitForCores = 0)
  expect_identical(clusters:::.clustersOption("waitForCores"), 0)
  expect_identical(eval(formals(clusters:::.allocateWhenAvailable)$waitSeconds), 0)
})

test_that("deoptimRescoreEvery re-scores every 10 generations unless the user says otherwise", {
  withr::local_options(clusters.deoptimRescoreEvery = NULL)
  expect_identical(clusters:::.clustersOption("deoptimRescoreEvery"), 10L)
})
