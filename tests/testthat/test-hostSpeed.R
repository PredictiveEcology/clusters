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

test_that("a slow host is kept whole when the total needs all its cores", {
  out <- suppressMessages(.excludeSlowHosts(nodesFor(c(a = 10, b = 10, c = 10)),
                                           speedsFor(c(a = 1, b = 1, c = 1.6)), total = 30, maxRatio = 1.25))
  expect_equal(out$free_est, c(10, 10, 10))
})

test_that("hosts without a record are kept, and absent records change nothing", {
  nodes <- nodesFor(c(a = 10, b = 10, c = 10))
  out <- suppressMessages(.excludeSlowHosts(nodes, speedsFor(c(a = 1.9)), total = 10))
  expect_equal(out$free_est, c(0, 10, 10))
  expect_message(out <- .excludeSlowHosts(nodes, NULL, total = 10), "No host speed records")
  expect_equal(out$free_est, c(10, 10, 10))
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

## Slow hosts are used last, and only for the shortfall.

test_that("a slow host is used only for the shortfall of the fast hosts", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  nodes <- nodesFor(c(a = 10, b = 10, c = 10))
  speeds <- speedsFor(c(a = 1, b = 1, c = 1.6))
  expect_message(out <- .excludeSlowHosts(nodes, speeds, total = 25, maxRatio = 1.25), "capped")
  expect_equal(out$free_est, c(10, 10, 5))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 20))$free_est, c(10, 10, 0))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 40))$free_est, c(10, 10, 10))
})

test_that("two slow hosts are added back least slow first", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  nodes <- nodesFor(c(a = 10, b = 10, c = 10, d = 10))
  speeds <- speedsFor(c(a = 1, b = 2, c = 1.5, d = 1))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 23))$free_est, c(10, 0, 3, 10))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 33))$free_est, c(10, 3, 10, 10))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 30))$free_est, c(10, 0, 10, 10))
})

test_that("the allocator never gives a capped host more than its cap, and fills fast hosts first", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  nodesA <- function(free, cores) data.frame(host = names(free), nodename = names(free),
                                             cores_total = cores, free_est = unname(free),
                                             stringsAsFactors = FALSE)
  for (total in c(21, 25, 30, 33, 35, 38, 50)) {
    nodes <- nodesA(c(a = 10, b = 20, c = 8), cores = c(20, 40, 16))
    out <- suppressMessages(.excludeSlowHosts(nodes, speedsFor(c(a = 1, b = 1, c = 2)), total = total))
    alloc <- .ht_allocate_min(out, total = total, beta = 0.5)
    expect_true(all(alloc$assign <= out$free_est))
    expect_equal(sum(alloc$assign), min(total, 38))
    expect_equal(sum(alloc$assign[1:2]), min(total, 30))
    expect_equal(alloc$assign[3], min(max(total - 30, 0), 8))
  }
})

test_that("rows with NaN or 0 speed are ignored, and a host with only such rows has no record", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  nodes <- nodesFor(c(a = 10, b = 10))
  speeds <- speedsFor(c(a = 3, b = 1), n = 100L)
  speeds$ratio[1] <- NaN
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(10, 10))
  speeds <- rbind(speedsFor(c(a = 3, a = 0)), speedsFor(c(b = 1)))
  speeds$median[2] <- 0
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(0, 10))
})

test_that("rows from a lightly used host do not clear its slow flag", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  nodes <- nodesFor(c(a = 10, b = 10))
  speeds <- rbind(speedsFor(c(a = 2, a = 1), n = c(100L, 5000L), host = "a"), speedsFor(c(b = 1)))
  speeds$workers <- c(28L, 4L, 28L)    # the large light-load row would average a to ~1.02
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(0, 10))
  speeds$workers <- c(28L, 20L, 28L)   # now the second row is a full load
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(10, 10))
  withr::local_options(clusters.hostSpeedFullLoad = 0.1)
  speeds$workers <- c(28L, 4L, 28L)
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(10, 10))
})

test_that("old rows without workers count as full load", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  nodes <- nodesFor(c(a = 10, b = 10))
  speeds <- speedsFor(c(a = 2, b = 1))
  expect_false("workers" %in% names(speeds))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(0, 10))
  speeds <- rbind(cbind(speeds[1, ], workers = 28L), speeds[1:2, ] |> transform(workers = NA_integer_))
  expect_equal(suppressMessages(.excludeSlowHosts(nodes, speeds, total = 10))$free_est, c(0, 10))
})

test_that(".recordHostSpeed stores the number of distinct workers per host", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  ev <- data.frame(seconds = c(1, 1, 1, 3), value = 0, host = c("a", "a", "a", "b"), pid = c(1L, 2L, 2L, 7L))
  .recordHostSpeed(list(ev), id = "w")
  got <- readRDS(.hostSpeedFile())
  expect_equal(got$workers[match(c("a", "b"), got$host)], c(2L, 1L))
})

test_that("the allocator assigns every free core when the total equals the free cores", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  set.seed(1)
  for (i in 1:200) {
    k <- sample(2:5, 1)
    cores <- sample(c(16, 32, 48, 64), k, replace = TRUE)
    free <- pmin(sample(1:60, k, replace = TRUE), cores)
    nodes <- data.frame(host = letters[seq_len(k)], cores_total = cores, free_est = free)
    expect_equal(.ht_allocate_min(nodes, total = sum(free), beta = 0.5)$assign, as.integer(free))
  }
})
