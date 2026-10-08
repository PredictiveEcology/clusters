## DEoptimIterative() saves per-host speeds beside the core ledger, with the workers each fit had per host.

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
  expect_setequal(names(got), c("host", "n", "median", "p90", "ratio", "time", "id", "workers"))
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
                                figurePath = FALSE, progressFile = FALSE, .plots = NULL, cachePath = cachePath,
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
                                figurePath = FALSE, progressFile = FALSE, .plots = NULL, cachePath = cachePath,
                                runName = "hs", .verbose = -1)))
  expect_equal(nrow(readRDS(file.path(d, "hostSpeed.rds"))), nrow(got))
})

## Slow hosts are used last, and only for the shortfall.

test_that(".recordHostSpeed stores the number of distinct workers per host", {
  ev <- data.frame(seconds = c(1, 1, 1, 3), value = 0, host = c("a", "a", "a", "b"), pid = c(1L, 2L, 2L, 7L))
  .recordHostSpeed(list(ev), id = "w")
  got <- readRDS(.hostSpeedFile())
  expect_equal(got$workers[match(c("a", "b"), got$host)], c(2L, 1L))
})
