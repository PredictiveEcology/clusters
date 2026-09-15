## DEoptimIterative2() runs DEoptim one generation at a time and caches each generation.
## FireSense phase 2 (2026-09-15) found three problems:
##   * the caller's NP and strategy were replaced by this function's defaults, so NP was
##     10 x parameters (120) whatever the cluster size, and strategy was always 3;
##   * every generation re-evaluated the carried population, 2 x NP evaluations for NP new ones;
##   * the per-generation Cache was skipped under spades.useCache = "eventsOnly", so a stopped fit
##     restarted from generation 1.
##
## What the lookup guarantees is that the population carried into a generation is not evaluated
## again. Evaluation counts are therefore upper bounds, not exact: a trial is occasionally
## bit-identical to a carried member (binomial crossover copies most components, and members share
## components inherited from a common ancestor), and then its known value is reused. A parameter set
## scored in an earlier generation but no longer in the population can be evaluated again, as plain
## DEoptim would.
##
## Counters are passed as plain variables: the per-generation cache key includes the objective
## function's enclosing environment, so an argument written as `counter = (counter <- new())`
## changes the key and a rerun misses the cache.

skip_if_not_installed("DEoptim")

lower <- c(a = 0, b = 0, c = 0)
upper <- c(a = 1, b = 1, c = 1)
pureFn <- function(par) sum((par - 0.3)^2)
parKey <- function(par) paste(sprintf("%a", as.numeric(par)), collapse = ",")

runDE <- function(itermax, cachePath, counter, NP = 8L, strategy = 2L, cluster = NULL) {
  fn <- function(par) {
    counter$n <- counter$n + 1L
    counter$keys <- c(counter$keys, parKey(par))
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

newCounter <- function() { counter <- new.env(); counter$n <- 0L; counter$keys <- character(0); counter }
resetCounter <- function(counter) { counter$n <- 0L; counter$keys <- character(0); invisible(counter) }

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
  counter <- newCounter()
  DE <- runDE(itermax = 2, cachePath = withr::local_tempdir(), counter = counter, NP = 8L, strategy = 2L)
  expect_identical(as.integer(seen$strategy), 2L)
  expect_identical(as.integer(seen$NP), 8L)
  expect_identical(nrow(DE[[2]]$member$pop), 8L)
})

test_that("after the first generation, each generation evaluates at most NP new parameter sets", {
  counter <- newCounter()
  DE <- runDE(itermax = 4, cachePath = withr::local_tempdir(), counter = counter, NP = 8L)
  ## generation 1: the random initial population (8) and its first trials (8); then at most 8 per
  ## generation. Re-evaluating the carried population, as before, cost 8 + 8 + 3 x 16 = 64.
  expect_lte(counter$n, 8L + 8L + 3L * 8L)
  expect_gt(counter$n, 8L)                              # the random initial population, at least
})

test_that("the population carried into a generation is not evaluated again", {
  cp <- withr::local_tempdir()
  counter <- newCounter()
  DE <- runDE(itermax = 1, cachePath = cp, counter = counter, NP = 8L)
  for (g in 2:4) {
    carried <- apply(DE[[g - 1L]]$member$pop, 1, parKey)
    resetCounter(counter)
    DE <- runDE(itermax = g, cachePath = cp, counter = counter, NP = 8L)  # 1..g-1 from the cache
    expect_lte(counter$n, 8L)
    expect_false(any(counter$keys %in% carried), label = paste("generation", g))
  }
})

test_that("the values carried into the next generation are the objective function's values", {
  counter <- newCounter()
  DE <- runDE(itermax = 3, cachePath = withr::local_tempdir(), counter = counter, NP = 8L)
  last <- DE[[3]]
  expect_equal(last$member$popval, unname(apply(last$member$pop, 1, pureFn)))
  expect_equal(min(last$member$popval), last$optim$bestval)
})

test_that("a stopped run resumes from its cached generations without evaluating them again", {
  cp <- withr::local_tempdir()
  counter <- newCounter()
  first <- runDE(itermax = 3, cachePath = cp, counter = counter, NP = 8L)
  expect_lte(counter$n, 8L + 8L + 2L * 8L)

  resetCounter(counter)
  again <- runDE(itermax = 3, cachePath = cp, counter = counter, NP = 8L)
  expect_identical(counter$n, 0L)                       # every generation came from the cache
  expect_identical(again[[3]]$member$pop, first[[3]]$member$pop)

  resetCounter(counter)
  longer <- runDE(itermax = 5, cachePath = cp, counter = counter, NP = 8L)
  expect_lte(counter$n, 2L * 8L)                        # only the two new generations were run
  expect_gt(counter$n, 0L)
  expect_identical(longer[[3]]$member$pop, first[[3]]$member$pop)
})

test_that("DEoptim settings passed to clusterSetup() reach DEoptim", {
  ## Eliot, 2026-09-15: "Any user passed args should pass into the DEoptim processes."
  ## clusterSetup() built control from itermax, trace, strategy, initialpop and NP only, so a
  ## caller's c, CR, F, p never reached DEoptim.
  seen <- new.env()
  realDEoptim <- DEoptim::DEoptim
  testthat::local_mocked_bindings(
    DEoptim = function(fn, lower, upper, control, ...) {
      seen$control <- control
      realDEoptim(fn, lower, upper, control, ...)
    },
    .package = "DEoptim")
  args <- list(c = 0.9, CR = 0.7, F = 0.6, strategy = 6L, p = 0.3)
  control <- suppressMessages(clusters::clusterSetup(
    itermax = 2, trace = FALSE, NP = 8L, cores = NULL, logPath = withr::local_tempdir(),
    objsNeeded = character(0), controlArgs = args))
  fn <- function(par) sum((par - 0.3)^2)
  withr::local_options(reproducible.cachePath = withr::local_tempdir(), reproducible.useCache = FALSE)
  DE <- suppressWarnings(suppressMessages(clusters:::DEoptimIterative2(
    fn, lower = lower, upper = upper, control = control,
    figurePath = FALSE, .plots = NULL, runName = "ctl", .verbose = -1)))
  for (nm in names(args)) expect_equal(seen$control[[nm]], args[[nm]], label = nm)
  expect_identical(as.integer(seen$control$NP), 8L)
})

test_that("a DEoptim setting clusterSetup() does not know is an error, not silently dropped", {
  expect_error(
    clusters::clusterSetup(itermax = 2, cores = NULL, logPath = withr::local_tempdir(),
                           objsNeeded = character(0), controlArgs = list(cc = 0.9)),
    "Not DEoptim.control\\(\\) settings: cc")
})

test_that("NP is exactly the number of workers in the cluster", {
  expect_identical(clusters:::.clusterNP(NP = NULL, nWorkers = 57L), 57L)
  expect_message(np <- clusters:::.clusterNP(NP = 100L, nWorkers = 57L), "57")
  expect_identical(np, 57L)
  expect_error(clusters:::.clusterNP(NP = NULL, nWorkers = 3L), "at least 4")
  ## no cluster: the requested NP stands
  expect_identical(clusters:::.clusterNP(NP = 120L, nWorkers = 0L), 120L)
})

test_that("with a cluster, each generation after the first evaluates at most NP sets on the workers", {
  skip_on_cran()
  skip_if(identical(Sys.getenv("NOT_CRAN"), ""), "PSOCK workers need the installed package")
  ## DEoptim needs NP >= 4, one member per worker, so the cluster cannot shrink below 4 workers.
  ## R CMD check allows only 2 child processes (_R_CHECK_LIMIT_CORES_): skip there, run locally.
  limit <- tolower(Sys.getenv("_R_CHECK_LIMIT_CORES_"))
  skip_if(nzchar(limit) && !identical(limit, "false"),
          "R CMD check limits child processes to 2; this test needs 4 workers")
  skip_if(isTRUE(parallel::detectCores() < 4L), "needs 4 cores")
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
  ## re-evaluating the carried population, as before, cost 4 + 4 + 2 x 8 = 24
  expect_lte(calls, 4L + 4L + 2L * 4L)
  expect_gt(calls, 4L)                                  # the random initial population, at least
  expect_identical(nrow(DE[[3]]$member$pop), length(cl))
})
