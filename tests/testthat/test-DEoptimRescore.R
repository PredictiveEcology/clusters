## Periodic re-scoring of the surviving population.
##
## DE never evaluates a surviving member again, so on a noisy objective one lucky low draw stays in the
## population for good and keeps beating trials that are really better. FireSense, 2026-09-16: every
## member of two converged fits (ELFs 6.2.1 and 4.2.2) was re-scored 10 times. The values DEoptim held
## were 275-566 below the members' replicated means (noise SD ~200-250), the gap grew with every
## generation, and on 4.2.2 DEoptim's best was only third best by replicated mean.
##
## With `clusters.deoptimRescoreEvery = k`, every k generations each member is evaluated once more and
## its value becomes the running mean of all its evaluations, so a lucky draw is diluted instead of kept.

skip_if_not_installed("DEoptim")

lower <- c(a = 0, b = 0, c = 0)
upper <- c(a = 1, b = 1, c = 1)

test_that(".rescoreUpdate() keeps carried members' sums and starts replaced members afresh", {
  prev <- list(keys = c("a", "b"), n = c(2L, 1L), sum = c(20, 7))
  ## `a` survived, so DEoptim holds its mean (10); `c` replaced `b` with a new evaluation (5)
  s <- .rescoreUpdate(prev, keys = c("a", "c"), popval = c(10, 5))
  expect_identical(s$keys, c("a", "c"))
  expect_identical(s$n, c(2L, 1L))
  expect_equal(s$sum, c(20, 5))
  ## one more evaluation of each
  s <- .rescoreAdd(s, keys = c("a", "c"), vals = c(13, 7))
  expect_identical(s$n, c(3L, 2L))
  expect_equal(.rescoreMeans(s, c("a", "c")), c(11, 6))
})

test_that(".rescoreUpdate() leaves members without a value unscored", {
  s <- .rescoreUpdate(NULL, keys = c("a", "b"), popval = c(4, NA))
  expect_identical(s$n, c(1L, 0L))
  expect_true(is.na(.rescoreMeans(s, "b")))
})

## A noisy objective: each call is recorded with the `thresh` it was given.
noisyObjective <- function(seen) {
  force(seen)
  function(par, thresh = 1) {
    seen$thresh <- c(seen$thresh, thresh)
    sum((par - 0.3)^2) + stats::rnorm(1, sd = 0.05)
  }
}

runRescored <- function(itermax, every, cachePath, seen, rescoreArgs = list()) {
  withr::local_options(reproducible.cachePath = cachePath, reproducible.useCache = FALSE,
                       clusters.cacheDEoptimIterations = TRUE,
                       clusters.deoptimRescoreEvery = every, clusters.deoptimRescoreArgs = rescoreArgs)
  withr::local_seed(1)
  msgs <- testthat::capture_messages(suppressWarnings(
    DE <- clusters:::DEoptimIterative2(noisyObjective(seen), lower = lower, upper = upper,
                                       control = list(NP = 8L, strategy = 2L, itermax = itermax, trace = FALSE),
                                       figurePath = FALSE, .plots = NULL, cachePath = cachePath,
                                       runName = "rescore", .verbose = -1)))
  list(DE = DE, messages = msgs)
}

newSeen <- function() { e <- new.env(); e$thresh <- numeric(0); e }

test_that("every k generations the population is re-scored and holds running means", {
  seen <- newSeen()
  out <- runRescored(itermax = 4, every = 2, cachePath = withr::local_tempdir(), seen = seen)
  expect_length(grep("Re-scored", out$messages), 2L)       # after generations 2 and 4
  m <- out$DE[[2]]$member
  expect_true(all(m$popvalN >= 2L))                        # every member now has two evaluations or more
  expect_equal(m$popval, m$popvalSum / m$popvalN)
  expect_null(out$DE[[1]]$member$popvalN)                  # nothing re-scored after generation 1
})

test_that("re-scoring uses clusters.deoptimRescoreArgs, and only for the re-scoring calls", {
  seen <- newSeen()
  runRescored(itermax = 2, every = 2, cachePath = withr::local_tempdir(), seen = seen,
              rescoreArgs = list(thresh = Inf))
  expect_identical(sum(is.infinite(seen$thresh)), 8L)      # one re-score per member
  expect_gt(sum(seen$thresh == 1), 8L)                     # DEoptim's own evaluations keep the default
})

test_that("a stopped re-scored run resumes from the cache to the same population and values", {
  cp <- withr::local_tempdir()
  first <- runRescored(itermax = 4, every = 2, cachePath = cp, seen = newSeen())
  seen <- newSeen()
  again <- runRescored(itermax = 4, every = 2, cachePath = cp, seen = seen)
  expect_length(seen$thresh, 0L)                           # nothing evaluated: chunks and re-scores replayed
  expect_identical(again$DE[[4]]$member$pop, first$DE[[4]]$member$pop)
  expect_identical(again$DE[[4]]$member$popval, first$DE[[4]]$member$popval)
})

test_that("re-scoring is off by default", {
  seen <- newSeen()
  withr::local_options(clusters.deoptimRescoreEvery = NULL)
  out <- runRescored(itermax = 3, every = NULL, cachePath = withr::local_tempdir(), seen = seen)
  expect_length(grep("Re-scored", out$messages), 0L)
  expect_null(out$DE[[3]]$member$popvalN)
})
