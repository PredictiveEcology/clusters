## DEoptimIterative() saves per-host speeds beside the core ledger; the next build leaves slow hosts out
## when the other hosts can still hold the population.

nodesFor <- function(free) data.frame(host = paste0("ssh", names(free)), nodename = names(free),
                                      free_est = unname(free), stringsAsFactors = FALSE)
speedsFor <- function(ratio, n = 100L, ageDays = 0, host = names(ratio))
  data.frame(host = host, n = n, median = 1, p90 = 2, ratio = unname(ratio),
             time = Sys.time() - ageDays * 86400, id = "r", stringsAsFactors = FALSE)

test_that("a slow host is left out when the others can hold the population", {
  out <- suppressMessages(.excludeSlowHosts(nodesFor(c(a = 10, b = 10, c = 10)),
                                           speedsFor(c(a = 1, b = 1, c = 1.6)), total = 20, maxRatio = 1.25))
  expect_equal(out$free_est, c(10, 10, 0))
  expect_message(.excludeSlowHosts(nodesFor(c(a = 10, c = 10)), speedsFor(c(a = 1, c = 1.6)), total = 10),
                 "c=1.6")
})

test_that("a slow host is kept when leaving it out would leave too few cores", {
  out <- suppressMessages(.excludeSlowHosts(nodesFor(c(a = 10, b = 10, c = 10)),
                                           speedsFor(c(a = 1, b = 1, c = 1.6)), total = 25, maxRatio = 1.25))
  expect_equal(out$free_est, c(10, 10, 10))
})

test_that("the slowest host goes first, and exclusion stops when the next would leave too few", {
  nodes <- nodesFor(c(a = 10, b = 10, c = 10, d = 10))
  speeds <- speedsFor(c(a = 1, b = 1.5, c = 2, d = 1))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 30))$free_est, c(10, 10, 0, 10))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 20))$free_est, c(10, 0, 0, 10))
})

test_that("hosts without a record are kept, and absent records change nothing", {
  nodes <- nodesFor(c(a = 10, b = 10, c = 10))
  out <- suppressMessages(.excludeSlowHosts(nodes, speedsFor(c(a = 1.9)), total = 10))
  expect_equal(out$free_est, c(0, 10, 10))
  expect_message(out <- .excludeSlowHosts(nodes, NULL, total = 10), "No host speed records")
  expect_equal(out$free_est, c(10, 10, 10))
})

test_that("a ratio that is not a number (all evaluations took 0 s) is no record, not a block", {
  ## 2026-10-01: test runs recorded median 0, ratio NaN; one such row made the host's mean NaN
  nodes <- nodesFor(c(a = 10, b = 10, c = 10))
  speeds <- speedsFor(c(NaN, 2, NaN, Inf), host = c("a", "a", "b", "c"))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(0, 10, 10))
})

test_that("recorded ratios are averaged weighted by n, and old records are ignored", {
  nodes <- nodesFor(c(a = 10, b = 10))
  ## a: (900 * 1 + 100 * 3) / 1000 = 1.2, not slow; with n = 100 for the first row it is 2, slow
  speeds <- speedsFor(c(1, 3, 1), n = c(900L, 100L, 100L), host = c("a", "a", "b"))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(10, 10))
  speeds$n[1] <- 100L
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(0, 10))
  speeds$time[2] <- Sys.time() - 40 * 86400
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(10, 10))
  withr::local_options(clusters.hostSpeedDays = 50)
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(0, 10))
})

mkEv <- function(host, sec) data.frame(seconds = sec, value = 0, host = host, pid = 1L)

test_that("recording appends rows, trims by age, and skips evaluations without host", {
  d <- withr::local_tempdir()
  withr::local_options(clusters.reservationsPath = d, clusters.hostSpeedDays = 30)
  file <- file.path(d, "hostSpeed.rds")
  suppressMessages(.recordHostSpeed(list(data.frame(seconds = 1, value = 0)), id = "none"))
  expect_false(file.exists(file))
  .recordHostSpeed(list(mkEv(c("a", "a", "b"), c(1, 1, 3))), id = "r1")
  suppressMessages(.recordHostSpeed(list(mkEv("a", 1)), id = "r2"))
  got <- readRDS(file)
  expect_setequal(names(got), c("host", "n", "median", "p90", "ratio", "time", "id"))
  expect_equal(got$id, c("r1", "r1", "r2"))
  expect_equal(got$host, c("b", "a", "a"))
  ## age out the first run
  got$time[got$id == "r1"] <- Sys.time() - 40 * 86400
  saveRDS(got, file)
  suppressMessages(.recordHostSpeed(list(mkEv("a", 1)), id = "r3"))
  expect_equal(readRDS(file)$id, c("r2", "r3"))
  ## the same chunk recorded again replaces its rows
  suppressMessages(.recordHostSpeed(list(mkEv(c("a", "b"), c(1, 2))), id = "r3"))
  got <- readRDS(file)
  expect_equal(got$id, c("r2", "r3", "r3"))
  expect_setequal(got$host[got$id == "r3"], c("a", "b"))
})

test_that("the end-of-fit table is shown once for all evaluations", {
  expect_message(.showHostSpeed(list(mkEv(c("a", "b"), c(1, 3)), mkEv("a", 1))), "Evaluation speed")
  expect_silent(.showHostSpeed(list(data.frame(seconds = 1, value = 0))))
})

test_that("a failed write is a warning, not an error", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  testthat::local_mocked_bindings(.hostSpeedFile = function(...) stop("disk full"))
  expect_warning(suppressMessages(.recordHostSpeed(list(mkEv("a", 1)), id = "r")), "disk full")
})

test_that("DEoptimIterative() on a local PSOCK cluster writes hostSpeed.rds", {
  skip_on_cran()
  skip_if_not_installed("DEoptim")
  d <- withr::local_tempdir()
  cachePath <- withr::local_tempdir()
  withr::local_options(clusters.reservationsPath = d, reproducible.cachePath = cachePath)
  cl <- localTestCluster(2)
  fn <- function(par) sum((par - 0.3)^2)
  suppressWarnings(suppressMessages(
    clusters:::DEoptimIterative(fn, lower = c(a = 0, b = 0), upper = c(a = 1, b = 1),
                                control = list(NP = 8L, itermax = 2L, trace = FALSE, cluster = cl),
                                figurePath = FALSE, .plots = NULL, cachePath = cachePath,
                                runName = "hs", .verbose = -1)))
  got <- readRDS(file.path(d, "hostSpeed.rds"))
  expect_true(all(got$host == Sys.info()[["nodename"]]))   # one row per computed chunk
  expect_true(all(got$n > 0L))
  expect_match(got$id, "^hs_[0-9]+$")
  expect_false(anyDuplicated(got[c("id", "host")]) > 0)
  ## a rerun replays the cached chunks: nothing was computed, so nothing is added
  suppressWarnings(suppressMessages(
    clusters:::DEoptimIterative(fn, lower = c(a = 0, b = 0), upper = c(a = 1, b = 1),
                                control = list(NP = 8L, itermax = 2L, trace = FALSE, cluster = cl),
                                figurePath = FALSE, .plots = NULL, cachePath = cachePath,
                                runName = "hs", .verbose = -1)))
  expect_equal(nrow(readRDS(file.path(d, "hostSpeed.rds"))), nrow(got))
})
