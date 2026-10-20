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
    DE <- clusters:::DEoptimIterative(noisyObjective(seen), lower = lower, upper = upper,
                                       control = list(NP = 8L, strategy = 2L, itermax = itermax, trace = FALSE),
                                       figurePath = FALSE, progressFile = FALSE, .plots = NULL, cachePath = cachePath,
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

test_that("with the option unset the population is re-scored every 10 generations", {
  out <- runRescored(itermax = 10, every = NULL, cachePath = withr::local_tempdir(), seen = newSeen())
  expect_length(grep("Re-scored", out$messages), 1L)       # after generation 10
  expect_null(out$DE[[9]]$member$popvalN)
  expect_true(all(out$DE[[10]]$member$popvalN >= 2L))
})

test_that("clusters.deoptimRescoreEvery = 0 turns re-scoring off", {
  out <- runRescored(itermax = 10, every = 0, cachePath = withr::local_tempdir(), seen = newSeen())
  expect_length(grep("Re-scored", out$messages), 0L)
  expect_null(out$DE[[10]]$member$popvalN)
})

## A population with nothing to re-score (every member holds the fail value) must not reach the
## objective: apply() and parApply() on a 0-row matrix call FUN once, in the master, on a vector of
## zeros. FireSense ELF 6.6.2 (2026-10-20): the master evaluated par = c(0, 0, ...) without the data
## the workers hold and died in the objective's fallback, "object 'annualDTx1000' not found".

mustNotRun <- function(par, ...) stop("the objective must not be called with ", paste(par, collapse = ","))
noRows <- matrix(numeric(0), nrow = 0, ncol = 3)

test_that(".rescorePopulation() with no rows calls nothing and returns no values", {
  expect_identical(.rescorePopulation(mustNotRun, pop = noRows, args = list(), parNames = c("a", "b", "c"), cl = NULL),
                   numeric(0))
  cl <- localTestCluster(1)
  expect_identical(.rescorePopulation(mustNotRun, pop = noRows, args = list(), parNames = c("a", "b", "c"), cl = cl),
                   numeric(0))
})

test_that("a population every member of which failed is not re-scored, and the fit stops", {
  seen <- new.env(); seen$pars <- list()
  allFail <- function(par, thresh = 1) { seen$pars <- c(seen$pars, list(par)); 1e6 }
  withr::local_options(reproducible.cachePath = withr::local_tempdir(), reproducible.useCache = FALSE,
                       clusters.cacheDEoptimIterations = TRUE, clusters.deoptimRescoreEvery = 1)
  withr::local_seed(1)
  msgs <- character()
  err <- withCallingHandlers(
    tryCatch(suppressWarnings(clusters:::DEoptimIterative(allFail, lower = lower, upper = upper,
                                         control = list(NP = 8L, strategy = 2L, itermax = 3, trace = FALSE),
                                         figurePath = FALSE, progressFile = FALSE, .plots = NULL,
                                         cachePath = getOption("reproducible.cachePath"), runName = "allfail", .verbose = -1)),
             error = function(e) e),
    message = function(m) { msgs <<- c(msgs, conditionMessage(m)); invokeRestart("muffleMessage") })
  expect_s3_class(err, "deoptimNoFeasibleMember")
  expect_match(conditionMessage(err), "every member of the population holds the fail value")
  expect_identical(err$generations, 1L)
  expect_length(grep("Nothing to re-score at generation 1", msgs), 1L)
  expect_length(grep("Re-scored", msgs), 0L)
  ## the 8 initial members and generation 1's 8 trials were evaluated, none a zero vector from a 0-row re-score
  expect_length(seen$pars, 16L)
  expect_false(any(vapply(seen$pars, function(p) all(p == 0), logical(1))))
})

test_that("a first generation with one scored member goes on as before", {
  oneGood <- function(par, thresh = 1) if (par[1] < 0.5) sum((par - 0.3)^2) else 1e6 # DEoptim passes par unnamed
  withr::local_options(reproducible.cachePath = withr::local_tempdir(), reproducible.useCache = FALSE,
                       clusters.cacheDEoptimIterations = TRUE, clusters.deoptimRescoreEvery = 0)
  withr::local_seed(1)
  DE <- suppressWarnings(suppressMessages(clusters:::DEoptimIterative(oneGood, lower = lower, upper = upper,
                                   control = list(NP = 8L, strategy = 2L, itermax = 2, trace = FALSE),
                                   figurePath = FALSE, progressFile = FALSE, .plots = NULL,
                                   cachePath = getOption("reproducible.cachePath"), runName = "onegood", .verbose = -1)))
  expect_length(DE, 2L)
  expect_true(any(DE[[2]]$member$popval < 1e6))
})
