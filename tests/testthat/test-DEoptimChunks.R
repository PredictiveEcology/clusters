## DEoptimIterative2() runs `iterStep` generations in each DEoptim call, and records how long each
## evaluation takes.
##
## FireSense, 2026-09-15: fireSenseUtils::runDEoptim documents iterStep as the number of generations per
## DEoptim call, but DEoptimIterative2 always ran one. DEoptim resets its CR/F adaptation (c > 0) at the
## start of every call (DEoptim src/de4_0.c), so one generation per call threw that adaptation away.
## Eliot: one parameter for the chunk and the plotting interval. Per-evaluation times show how long the
## slowest evaluations take -- they set the time of a generation -- to tune the long-tail cutoff.

skip_if_not_installed("DEoptim")

lower <- c(a = 0, b = 0, c = 0)
upper <- c(a = 1, b = 1, c = 1)

## Records the itermax of every DEoptim call; returns the run and its messages.
runChunked <- function(itermax, iterStep, cachePath, calls, fn = function(par) sum((par - 0.3)^2)) {
  realDEoptim <- DEoptim::DEoptim
  testthat::local_mocked_bindings(
    DEoptim = function(fn, lower, upper, control, ...) {
      calls$itermax <- c(calls$itermax, as.integer(control$itermax))
      realDEoptim(fn, lower, upper, control, ...)
    },
    .package = "DEoptim")
  withr::local_options(reproducible.cachePath = cachePath,
                       reproducible.useCache = FALSE,       # what spades.useCache = "eventsOnly" does
                       clusters.cacheDEoptimIterations = TRUE)
  msgs <- testthat::capture_messages(suppressWarnings(
    DE <- clusters:::DEoptimIterative2(fn, lower = lower, upper = upper,
                                       control = list(NP = 8L, strategy = 2L, itermax = itermax, trace = FALSE),
                                       iterStep = iterStep, figurePath = FALSE, .plots = NULL,
                                       cachePath = cachePath, runName = "chunks", .verbose = -1)))
  list(DE = DE, messages = msgs)
}

newCalls <- function() { calls <- new.env(); calls$itermax <- integer(0); calls }
generations <- function(DE) sum(vapply(DE, function(d) length(d$member$bestvalit), integer(1)))

test_that("iterStep generations run in each DEoptim call, and together they cover itermax", {
  calls <- newCalls()
  out <- runChunked(itermax = 7, iterStep = 3, cachePath = withr::local_tempdir(), calls = calls)
  expect_identical(calls$itermax, c(3L, 3L, 1L))
  expect_length(out$DE, 3L)
  expect_identical(generations(out$DE), 7L)
})

test_that("without iterStep each DEoptim call runs one generation, as before", {
  calls <- newCalls()
  out <- runChunked(itermax = 3, iterStep = NULL, cachePath = withr::local_tempdir(), calls = calls)
  expect_identical(calls$itermax, c(1L, 1L, 1L))
  expect_identical(generations(out$DE), 3L)
})

test_that("a stopped chunked run resumes from its cached chunks, and another iterStep does not reuse them", {
  cp <- withr::local_tempdir()
  first <- runChunked(itermax = 6, iterStep = 3, cachePath = cp, calls = newCalls())

  calls <- newCalls()
  again <- runChunked(itermax = 6, iterStep = 3, cachePath = cp, calls = calls)
  expect_length(calls$itermax, 0L)                      # both chunks came from the cache
  expect_identical(again$DE[[2]]$member$pop, first$DE[[2]]$member$pop)

  ## the first 2-generation chunk starts from the same (random) population as the first 3-generation
  ## chunk, so only the chunk length tells them apart in the cache key
  calls <- newCalls()
  runChunked(itermax = 6, iterStep = 2, cachePath = cp, calls = calls)
  expect_identical(calls$itermax, c(2L, 2L, 2L))
})

test_that("itermax given as 4 or 4L finds the same cached chunks", {
  ## the chunk length is in the cache key; computed from a double itermax it was a double, from an
  ## integer an integer, and those digest differently (test-DEoptimIterative2 passes 1, then 2:4)
  cp <- withr::local_tempdir()
  runChunked(itermax = 4, iterStep = 2, cachePath = cp, calls = newCalls())
  calls <- newCalls()
  runChunked(itermax = 4L, iterStep = 2L, cachePath = cp, calls = calls)
  expect_length(calls$itermax, 0L)
})

test_that("every new evaluation's time is recorded, and each chunk reports the times", {
  ## 50 ms each, checked against 30 ms: Windows' timer ticks about every 15 ms, and a 10 ms sleep there
  ## was recorded as less than 9 ms (CI, R CMD check on windows-latest)
  slowFn <- function(par) { Sys.sleep(0.05); sum((par - 0.3)^2) }
  out <- runChunked(itermax = 4, iterStep = 2, cachePath = withr::local_tempdir(), calls = newCalls(),
                    fn = slowFn)
  ev <- out$DE[[1]]$member$evaluations
  expect_s3_class(ev, "data.frame")
  expect_named(ev, c("seconds", "value"))
  ## chunk 1: the random initial population (8) and at most 8 trials in each of its 2 generations
  expect_gt(nrow(ev), 8L)
  expect_lte(nrow(ev), 8L + 2L * 8L)
  expect_true(all(ev$seconds >= 0.03))
  expect_true(all(is.finite(ev$value)))
  expect_lte(nrow(out$DE[[2]]$member$evaluations), 2L * 8L)   # later chunks: trials only
  expect_true(any(grepl("evaluations: .* s \\(min / median / 90% / max\\); .* s per generation",
                        out$messages)))
})
