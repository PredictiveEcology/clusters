## DEoptimIterative2() runs DEoptim one generation at a time and caches each generation.
## FireSense phase 2 (2026-09-15) found three problems:
##   * the caller's NP and strategy were replaced by this function's defaults, so NP was
##     10 x parameters (120) whatever the cluster size, and strategy was always 3;
##   * every generation re-evaluated the carried population, 2 x NP evaluations for NP new ones;
##   * the per-generation Cache was skipped under spades.useCache = "eventsOnly", so a stopped fit
##     restarted from generation 1.

skip_if_not_installed("DEoptim")

lower <- c(a = 0, b = 0, c = 0)
upper <- c(a = 1, b = 1, c = 1)
pureFn <- function(par) sum((par - 0.3)^2)

runDE <- function(itermax, cachePath, counter, NP = 8L, strategy = 2L, cluster = NULL) {
  fn <- function(par) {
    counter$n <- counter$n + 1L
    sum((par - 0.3)^2)
  }
  control <- list(NP = NP, strategy = strategy, itermax = itermax, trace = FALSE)
  if (!is.null(cluster)) control$cluster <- cluster
  withr::local_options(reproducible.cachePath = cachePath,
                       ## what spades.useCache = "eventsOnly" does to nested Cache() calls
                       reproducible.useCache = FALSE)
  suppressWarnings(suppressMessages(
    clusters:::DEoptimIterative2(fn, lower = lower, upper = upper, control = control,
                                 figurePath = FALSE, .plots = NULL, cachePath = cachePath,
                                 runName = "test", .verbose = -1)
  ))
}

test_that("the caller's NP and strategy reach DEoptim", {
  seen <- new.env()
  realDEoptim <- DEoptim::DEoptim
  testthat::local_mocked_bindings(
    DEoptim = function(fn, lower, upper, control, ...) {
      seen$strategy <- control$strategy
      seen$NP <- control$NP
      realDEoptim(fn, lower, upper, control, ...)
    },
    .package = "DEoptim")
  counter <- new.env(); counter$n <- 0L
  DE <- runDE(itermax = 2, cachePath = withr::local_tempdir(), counter = counter, NP = 8L, strategy = 2L)
  expect_identical(as.integer(seen$strategy), 2L)
  expect_identical(as.integer(seen$NP), 8L)
  expect_identical(nrow(DE[[2]]$member$pop), 8L)
})

test_that("after the first generation, each generation evaluates NP new parameter sets, not 2 x NP", {
  counter <- new.env(); counter$n <- 0L
  DE <- runDE(itermax = 4, cachePath = withr::local_tempdir(), counter = counter, NP = 8L)
  ## generation 1: the random initial population (8) and its first trials (8); then 8 per generation
  expect_identical(counter$n, 8L + 8L + 3L * 8L)
})

test_that("the values carried into the next generation are the objective function's values", {
  counter <- new.env(); counter$n <- 0L
  DE <- runDE(itermax = 3, cachePath = withr::local_tempdir(), counter = counter, NP = 8L)
  last <- DE[[3]]
  expect_equal(last$member$popval, unname(apply(last$member$pop, 1, pureFn)))
  expect_equal(min(last$member$popval), last$optim$bestval)
})

test_that("a stopped run resumes from its cached generations without evaluating them again", {
  cp <- withr::local_tempdir()
  counter <- new.env(); counter$n <- 0L
  first <- runDE(itermax = 3, cachePath = cp, counter = counter, NP = 8L)
  expect_identical(counter$n, 8L + 8L + 2L * 8L)

  counter$n <- 0L
  again <- runDE(itermax = 3, cachePath = cp, counter = counter, NP = 8L)
  expect_identical(counter$n, 0L)                       # every generation came from the cache
  expect_identical(again[[3]]$member$pop, first[[3]]$member$pop)

  counter$n <- 0L
  longer <- runDE(itermax = 5, cachePath = cp, counter = counter, NP = 8L)
  expect_identical(counter$n, 2L * 8L)                  # only the two new generations were run
  expect_identical(longer[[3]]$member$pop, first[[3]]$member$pop)
})

test_that("NP is exactly the number of workers in the cluster", {
  expect_identical(clusters:::.clusterNP(NP = NULL, nWorkers = 57L), 57L)
  expect_message(np <- clusters:::.clusterNP(NP = 100L, nWorkers = 57L), "57")
  expect_identical(np, 57L)
  expect_error(clusters:::.clusterNP(NP = NULL, nWorkers = 3L), "at least 4")
  ## no cluster: the requested NP stands
  expect_identical(clusters:::.clusterNP(NP = 120L, nWorkers = 0L), 120L)
})

test_that("with a cluster, each generation after the first evaluates NP sets on the workers", {
  skip_on_cran()
  skip_if(identical(Sys.getenv("NOT_CRAN"), ""), "PSOCK workers need the installed package")
  cl <- parallel::makeCluster(4L)
  on.exit(parallel::stopCluster(cl), add = TRUE)
  ## the workers must load this version of clusters to run the recording wrapper
  parallel::clusterCall(cl, function(libs) .libPaths(libs), .libPaths())
  ok <- unlist(parallel::clusterCall(cl, function() {
    requireNamespace("clusters", quietly = TRUE) &&
      exists(".deoptimRecord", envir = asNamespace("clusters"), inherits = FALSE)
  }))
  skip_if_not(all(ok), "the installed clusters on the workers predates this change")
  parallel::clusterEvalQ(cl, .nCalls <- 0L)

  fnWorker <- function(par) {
    assign(".nCalls", get(".nCalls", envir = globalenv()) + 1L, envir = globalenv())
    sum((par - 0.3)^2)
  }
  withr::local_options(reproducible.cachePath = withr::local_tempdir(), reproducible.useCache = FALSE)
  DE <- suppressWarnings(suppressMessages(clusters:::DEoptimIterative2(
    fnWorker, lower = lower, upper = upper,
    control = list(NP = 4L, strategy = 2L, itermax = 3, trace = FALSE, cluster = cl),
    figurePath = FALSE, .plots = NULL, runName = "cl", .verbose = -1)))
  calls <- sum(unlist(parallel::clusterEvalQ(cl, .nCalls)))
  expect_identical(calls, 4L + 4L + 2L * 4L)
  expect_identical(nrow(DE[[3]]$member$pop), length(cl))
})
