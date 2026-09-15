## DEoptimIterative2() runs `iterStep` generations in each DEoptim call; with iterStep > 1 it turns off DEoptim's CR/F
## adaptation (c = 0).
##
## DEoptim (2.2.8, src/de4_0.c) accumulates goodF only on successful trials, never resets it within a call, and after
## every generation sets meanF = (1 - c) * meanF + c * goodF2 / goodF. When a call's first generation has no successful
## trial, goodF is 0, meanF is NaN from then on, and every later trial vector is NaN. A FireSense fit (2026-09-15,
## iterStep 5, c = 0.1) failed on all 110 workers at generation ~141 that way. One generation per call never reaches it.

skip_if_not_installed("DEoptim")

lower <- c(a = 0, b = 0, c = 0)
upper <- c(a = 1, b = 1, c = 1)

## An objective that no trial improves on: the starting population scores 1, every other parameter set 1e6.
noImprovementObjective <- function(init, seen) {
  keys <- apply(init, 1, function(p) paste(sprintf("%a", as.numeric(p)), collapse = ","))
  function(par) {
    if (any(!is.finite(par))) seen$nonFinite <- seen$nonFinite + 1L
    if (paste(sprintf("%a", as.numeric(par)), collapse = ",") %in% keys) 1 else 1e6
  }
}

startingPopulation <- function() {
  set.seed(1)
  matrix(runif(8 * 3), 8, 3)
}

newSeen <- function() { seen <- new.env(); seen$nonFinite <- 0L; seen$c <- numeric(0); seen }

runIterative <- function(iterStep, cc, seen) {
  init <- startingPopulation()
  realDEoptim <- DEoptim::DEoptim
  testthat::local_mocked_bindings(
    DEoptim = function(fn, lower, upper, control, ...) {
      seen$c <- c(seen$c, control$c)
      realDEoptim(fn, lower, upper, control, ...)
    },
    .package = "DEoptim")
  cachePath <- withr::local_tempdir()
  withr::local_options(reproducible.cachePath = cachePath, reproducible.useCache = FALSE,
                       clusters.cacheDEoptimIterations = FALSE)
  testthat::capture_messages(suppressWarnings(
    clusters:::DEoptimIterative2(noImprovementObjective(init, seen), lower = lower, upper = upper,
                                 control = list(NP = 8L, strategy = 6L, itermax = 6, trace = FALSE, c = cc,
                                                initialpop = init),
                                 iterStep = iterStep, figurePath = FALSE, .plots = NULL, cachePath = cachePath,
                                 runName = "cguard", .verbose = -1)))
}

test_that("the premise: DEoptim with c > 0 hands NaN trial parameters to the objective after a generation without success", {
  seen <- newSeen()
  init <- startingPopulation()
  suppressWarnings(DEoptim::DEoptim(noImprovementObjective(init, seen), lower, upper,
                                    control = DEoptim::DEoptim.control(NP = 8, itermax = 3, strategy = 6, c = 0.5,
                                                                       trace = FALSE, initialpop = init)))
  if (seen$nonFinite == 0L)
    skip("this DEoptim no longer produces NaN trial parameters; the c = 0 guard may be unnecessary")
  expect_gt(seen$nonFinite, 0L)
})

test_that("with iterStep > 1, c is set to 0 and the objective never gets non-finite parameters", {
  seen <- newSeen()
  msgs <- runIterative(iterStep = 3L, cc = 0.5, seen = seen)
  expect_identical(seen$nonFinite, 0L)
  expect_true(length(seen$c) > 0L && all(seen$c == 0))
  expect_true(any(grepl("c = 0", msgs, fixed = TRUE)))
})

test_that("with one generation per call the caller's c stands", {
  seen <- newSeen()
  runIterative(iterStep = 1L, cc = 0.5, seen = seen)
  expect_true(length(seen$c) > 0L && all(seen$c == 0.5))
  expect_identical(seen$nonFinite, 0L)
})
