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
  expect_setequal(names(got), c("host", "n", "median", "p90", "ratio", "time", "id", "workers", "booked"))
  expect_true(all(is.na(got$booked)))
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

test_that("rows written before the workers column existed are kept and the new record is written", {
  d <- withr::local_tempdir()
  withr::local_options(clusters.reservationsPath = d, clusters.hostSpeedDays = 30)
  file <- file.path(d, "hostSpeed.rds")
  saveRDS(data.frame(host = "old", n = 3L, median = 1, p90 = 2, ratio = 1, time = Sys.time(), id = "r0",
                     stringsAsFactors = FALSE), file)
  expect_no_warning(.recordHostSpeed(list(mkEv(c("a", "b"), c(1, 2))), id = "r1"))
  got <- readRDS(file)
  expect_equal(got$id, c("r0", "r1", "r1"))
  expect_true(is.na(got$workers[got$id == "r0"]))
  expect_false(anyNA(got$workers[got$id == "r1"]))
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

test_that("a chunk's id carries the run name, and a fit without one is named by its process", {
  expect_equal(.chunkId("hs", 3L), "hs_3")
  expect_equal(.chunkId(NULL, 3L), paste0("pid", Sys.getpid(), "_3"))
  expect_equal(.chunkId("", 3L), paste0("pid", Sys.getpid(), "_3"))
  expect_equal(.chunkId(12.3, 930L), "12.3_930")
})

test_that("the speeds file is read under the lock, or NULL without one", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  expect_null(.readHostSpeeds())
  .recordHostSpeed(list(mkEv(c("a", "b"), c(1, 2))), id = "r1")
  expect_equal(.readHostSpeeds()$id, c("r1", "r1"))
  saveRDS("not a table", .hostSpeedFile())
  expect_equal(.readHostSpeeds(), "not a table")   # .hostSpeedFactor() ignores what is not a data frame
})

test_that("each row records the workers every cluster had booked on the host, by nodename", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  cl <- structure(list(), class = "cluster")
  expect_null(.bookedByNodename(cl))                       # not built by plan_psock_min()
  attr(cl, "hostNodenames") <- c(pinus = "A127584", localhost = "A159568", abies = "A127585")
  expect_null(.bookedByNodename(cl))                       # nothing booked
  reserveCores(data.frame(host = c("pinus", "localhost"), assign = c(3L, 10L)))
  reserveCores(data.frame(host = c("pinus", "zzz"), assign = c(11L, 2L)))
  booked <- .bookedByNodename(cl)
  expect_equal(booked[c("A127584", "A159568")], c(A127584 = 14L, A159568 = 10L))
  expect_false("zzz" %in% names(booked))
  .recordHostSpeed(list(mkEv(c("A127584", "A127585"), c(1, 2))), id = "r1", booked = booked)
  got <- readRDS(.hostSpeedFile())
  expect_equal(got$booked[match(c("A127584", "A127585"), got$host)], c(14L, NA))
  ## a cluster whose reservation ledger cannot be read is no reason to fail the fit
  withr::local_options(clusters.useReservations = FALSE)
  expect_null(.bookedByNodename(cl))
})
