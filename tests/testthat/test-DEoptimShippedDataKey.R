## The data a fit runs on must be in its cache key.
##
## 2026-09-29, FireSense held-out folds: fireSenseUtils::runDEoptim() ships the fold's data to the
## workers through clusterSetup(objsNeeded = ...), not as objective arguments. The per-generation key
## in DEoptimIterative() was built from the objective's arguments only, so the two folds of an ELF
## had identical keys and shared every generation: one fold computed a generation, the other loaded
## it, and both ended with the same fit. clusterSetup() now digests the shipped objects once and
## DEoptimIterative() keys every generation on that digest.

skip_if_not_installed("DEoptim")

lower <- c(a = 0, b = 0)
upper <- c(a = 1, b = 1)

## The objective reads `target` from the global environment, where clusterSetup() puts the shipped
## objects for a local run -- the same way the workers see them.
fn <- function(par) sum((par - target)^2)

fitWith <- function(target, cachePath) {
  assign("target", target, envir = .GlobalEnv)
  withr::defer(rm("target", envir = .GlobalEnv))
  control <- clusters::clusterSetup(cores = NULL, objsNeeded = "target", envir = environment(), logPath = tempfile(),
                                    itermax = 6, strategy = 2L, NP = 8L, trace = FALSE)
  withr::local_options(reproducible.cachePath = cachePath, clusters.cacheDEoptimIterations = TRUE)
  set.seed(1) # the same starting population for both fits, as two folds of one ELF have
  DE <- suppressMessages(suppressWarnings(
    clusters::DEoptimIterative(fn, lower = lower, upper = upper, control = control,
                                figurePath = FALSE, cachePath = cachePath, runName = "fold",
                                .verbose = -1)))
  DE[[length(DE)]]$optim$bestmem
}

test_that("clusterSetup() carries one digest of the objects it ships, which differs with the data", {
  target <- c(0.2, 0.2)
  c1 <- clusters::clusterSetup(cores = NULL, objsNeeded = "target", envir = environment(), logPath = tempfile(), trace = FALSE)
  target <- c(0.8, 0.8)
  c2 <- clusters::clusterSetup(cores = NULL, objsNeeded = "target", envir = environment(), logPath = tempfile(), trace = FALSE)
  expect_false(is.null(shippedObjectsDigest(c1)))
  expect_false(identical(shippedObjectsDigest(c1), shippedObjectsDigest(c2)))
  rm("target", envir = .GlobalEnv)
})

test_that("two fits that differ only in the shipped data do not share cached generations", {
  cp <- withr::local_tempdir()
  best1 <- fitWith(c(0.2, 0.2), cp)
  best2 <- fitWith(c(0.8, 0.8), cp) # same seed, control, objective and runName: only the data differ
  ## each converges towards its own data; sharing generations would give the first fit's answer
  expect_true(all(abs(best1 - 0.2) < abs(best1 - 0.8)))
  expect_true(all(abs(best2 - 0.8) < abs(best2 - 0.2)))
  ## and the same data again is still a cache hit: the same answer, no new entries
  entries <- function() length(unique(reproducible::showCache(cp, verbose = -2)$cacheId))
  n <- entries()
  expect_equal(fitWith(c(0.2, 0.2), cp), best1)
  expect_identical(entries(), n)
})
