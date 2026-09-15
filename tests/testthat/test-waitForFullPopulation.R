## A cluster build waits for the whole population it asked for.
##
## FireSense phase 2 (2026-09-15): fits whose clusters were built after others got 5, 2 and 7
## of the 100 workers they asked for, and ran DEoptim with them for hours. The old wait loop
## only waited when no worker at all was free. A build now waits until the allocation can
## supply `total` (or `clusters.minWorkersFraction` of it), so the number of concurrent fits
## limits itself to what the cluster can hold, and gives up with an error at the deadline
## rather than starting small.

nodesWith <- function(free, cores = 48L, hosts = letters[seq_along(free)])
  data.frame(host = hosts, cores_total = cores, free_est = free, stringsAsFactors = FALSE)

## A fake clock: sleep() advances now().
fakeClock <- function() {
  e <- new.env()
  e$t <- as.POSIXct("2026-09-15 12:00:00", tz = "UTC")
  e$sleeps <- 0L
  list(now = function() e$t,
       sleep = function(s) { e$t <- e$t + s; e$sleeps <- e$sleeps + 1L; invisible(NULL) },
       sleeps = function() e$sleeps)
}

test_that("with enough free cores the full population is allocated without waiting", {
  clk <- fakeClock()
  alloc <- suppressMessages(clusters:::.allocateWhenAvailable(
    probe = function() nodesWith(c(48L, 48L)), total = 40L, beta = 0.5, minFraction = 1,
    waitSeconds = 3600, sleep = clk$sleep, now = clk$now))
  expect_identical(sum(alloc$assign), 40L)
  expect_identical(clk$sleeps(), 0L)
})

test_that("a build waits, re-probing, until the full population is free", {
  clk <- fakeClock()
  calls <- 0L
  probe <- function() {
    calls <<- calls + 1L
    if (calls < 3L) nodesWith(c(5L, 5L)) else nodesWith(c(48L, 48L))
  }
  msgs <- testthat::capture_messages(
    alloc <- clusters:::.allocateWhenAvailable(probe, total = 40L, beta = 0.5, minFraction = 1,
                                               waitSeconds = 3600, sleep = clk$sleep, now = clk$now))
  expect_identical(calls, 3L)
  expect_identical(clk$sleeps(), 2L)
  expect_identical(sum(alloc$assign), 40L)
  ## the wait says how far short it is
  expect_true(any(grepl("10 of 40", msgs, fixed = TRUE)))
})

test_that("it never starts with fewer workers: at the deadline it stops with the shortfall", {
  clk <- fakeClock()
  expect_error(
    suppressMessages(clusters:::.allocateWhenAvailable(
      probe = function() nodesWith(c(5L, 5L)), total = 40L, beta = 0.5, minFraction = 1,
      waitSeconds = 180, sleep = clk$sleep, now = clk$now)),
    "could only get 10 of 40 workers")
  expect_gte(clk$sleeps(), 3L)
})

test_that("without a wait (clusters.waitForCores = 0) a short allocation is an error, not a small cluster", {
  clk <- fakeClock()
  expect_error(
    suppressMessages(clusters:::.allocateWhenAvailable(
      probe = function() nodesWith(c(5L, 5L)), total = 40L, beta = 0.5, minFraction = 1,
      waitSeconds = 0, sleep = clk$sleep, now = clk$now)),
    "could only get 10 of 40 workers")
  expect_identical(clk$sleeps(), 0L)
})

test_that("clusters.minWorkersFraction allows a partial start when asked for", {
  clk <- fakeClock()
  alloc <- suppressMessages(clusters:::.allocateWhenAvailable(
    probe = function() nodesWith(c(10L, 10L)), total = 40L, beta = 0.5, minFraction = 0.5,
    waitSeconds = 3600, sleep = clk$sleep, now = clk$now))
  expect_identical(sum(alloc$assign), 20L)
  expect_identical(clk$sleeps(), 0L)
})

test_that("zero free cores waits like before, then stops at the deadline", {
  clk <- fakeClock()
  expect_error(
    suppressMessages(clusters:::.allocateWhenAvailable(
      probe = function() nodesWith(c(0L, 0L)), total = 40L, beta = 0.5, minFraction = 1,
      waitSeconds = 300, sleep = clk$sleep, now = clk$now)),
    "could only get 0 of 40 workers")
  expect_gte(clk$sleeps(), 5L)
})

test_that("a population larger than every core on the hosts stops at once instead of waiting", {
  clk <- fakeClock()
  expect_error(
    suppressMessages(clusters:::.allocateWhenAvailable(
      probe = function() nodesWith(c(16L, 16L), cores = 16L), total = 100L, beta = 0.5, minFraction = 1,
      waitSeconds = 8 * 3600, sleep = clk$sleep, now = clk$now)),
    "more workers than")
  expect_identical(clk$sleeps(), 0L)
})

test_that("plan_psock_min allocates through the waiting helper and still honours clusters.waitForCores", {
  src <- paste(deparse(clusters:::plan_psock_min), collapse = "\n")
  expect_match(src, ".allocateWhenAvailable(", fixed = TRUE)
  expect_match(src, "clusters.waitForCores", fixed = TRUE)
  expect_match(src, "clusters.minWorkersFraction", fixed = TRUE)
})
