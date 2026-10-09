## 2026-10-09: placement booked only cores. Host `core` (187 GB) was filled to 187.1 GB by DEoptim workers and
## hung for 1.5 h, killing every fit with a worker there. A host now gets only as many workers as its free
## memory holds, from what the same fit used last time (fitMemory.rds) or an estimate.

memNodes <- function(avail, total = 100, free = 40, host = letters[seq_along(avail)])
  data.frame(host = host, cores_total = 48L, free_est = free, loadavg_5 = 0,
             mem_available_gb = avail, mem_total_gb = total, stringsAsFactors = FALSE)

capacity <- function(nodes, ...)
  suppressMessages(clusters:::.fitCapacity(nodes, total = 80, load_memory = "5min", ...))

mkMem <- function(host, pid, gb) data.frame(seconds = 1, value = 0, host = host, pid = pid, peakGB = gb)

test_that("a host with free cores but little free memory is capped by its memory", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.keepFreeCores = 0)
  out <- capacity(memNodes(avail = c(30, 200)), memPerWorkerGB = 4)
  ## a: floor((30 - 10% of 100) / 4) = 5; b: floor((200 - 10) / 4) = 47, above its 40 free cores
  expect_equal(out$free_est, c(5, 40))
  expect_message(clusters:::.fitCapacity(memNodes(avail = c(30, 200)), 80, "5min", memPerWorkerGB = 4), "a=5")
  withr::local_options(clusters.memoryHeadroom = 0.2)
  expect_equal(capacity(memNodes(avail = 30), memPerWorkerGB = 4)$free_est, 2)
  ## nothing is capped when no memory per worker is known
  expect_equal(capacity(memNodes(avail = 30))$free_est, 40)
})

test_that("memory per worker comes from the latest fitMemory record of the same runName", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir())
  expect_equal(clusters:::.memPerWorkerGB("elf1", estimateGB = 2), 2)
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 5))), runName = "elf1", id = "elf1_1")
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 9))), runName = "elf2", id = "elf2_1")
  expect_equal(clusters:::.memPerWorkerGB("elf1", estimateGB = 2), 5)
  file <- clusters:::.fitMemoryFile()
  got <- readRDS(file)
  got$time[got$id == "elf1_1"] <- Sys.time() - 3600
  saveRDS(got, file)
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 3))), runName = "elf1", id = "elf1_2")
  expect_equal(clusters:::.memPerWorkerGB("elf1", estimateGB = 2), 3)   # the most recent, not the largest
  expect_equal(clusters:::.memPerWorkerGB("elf2", estimateGB = 2), 9)
  ## and it sets the cap
  withr::local_options(clusters.keepFreeCores = 0)
  expect_equal(capacity(memNodes(avail = 30), memPerWorkerGB = clusters:::.memPerWorkerGB("elf2", 2))$free_est, 2)
})

test_that("a booked memGB lowers another build's capacity within the grace window only", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.keepFreeCores = 0)
  id <- reserveCores(data.frame(host = "a", assign = 5L), memPerWorkerGB = 4)
  expect_equal(liveReservations()$memGB, 20)
  nodes <- memNodes(avail = 51)
  ## 51 - 20 booked = 31 free: floor((31 - 10) / 4) = 5, against 10 with nothing booked
  expect_equal(capacity(nodes, memPerWorkerGB = 4)$free_est, 5)
  ## the cluster asking where its own workers belong does not count its own booking
  expect_equal(capacity(nodes, memPerWorkerGB = 4, ownId = id)$free_est, 10)
  ## an hour on, its workers have grown and MemAvailable shows them
  file <- reservationsPath()
  res <- readRDS(file); res$created <- Sys.time() - 3600; saveRDS(res, file)
  expect_equal(capacity(nodes, memPerWorkerGB = 4)$free_est, 10)
})

test_that("a rebalancing cluster's own workers' memory is added back to the host's free memory", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.keepFreeCores = 0)
  out <- capacity(memNodes(avail = 30), memPerWorkerGB = 4, own = c(a = 5))
  expect_equal(out$free_est, 10)   # floor((30 + 5 * 4 - 10) / 4); without `own` it is 5
})

test_that("recording writes one row per chunk and replaces by id", {
  d <- withr::local_tempdir()
  withr::local_options(clusters.reservationsPath = d)
  ## two hosts, four workers; a worker's peak is its largest reading; NA where unreadable
  ev <- mkMem(c("a", "a", "a", "b", "b", "c"), c(1L, 1L, 2L, 7L, 8L, 9L), c(1, 3, 2, 4, 6, NA))
  clusters:::.recordFitMemory(list(ev), runName = "elf1", id = "elf1_1")
  got <- readRDS(file.path(d, "fitMemory.rds"))
  expect_equal(nrow(got), 1L)
  expect_setequal(names(got), c("runName", "id", "time", "workers", "memMedianGB", "memMaxGB"))
  expect_equal(got$workers, 4L)
  expect_equal(got$memMedianGB, median(c(3, 2, 4, 6)))
  expect_equal(got$memMaxGB, 6)
  clusters:::.recordFitMemory(list(mkMem("a", 1:2, c(1, 2))), runName = "elf1", id = "elf1_2")
  clusters:::.recordFitMemory(list(mkMem("a", 1:2, c(8, 9))), runName = "elf1", id = "elf1_1")
  got <- readRDS(file.path(d, "fitMemory.rds"))
  expect_equal(got$id, c("elf1_2", "elf1_1"))
  expect_equal(got$memMaxGB, c(2, 9))
  ## nothing readable, nothing recorded; a failed write is a warning
  expect_null(clusters:::.recordFitMemory(list(mkMem("a", 1L, NA_real_)), runName = "x", id = "x_1"))
  expect_null(clusters:::.recordFitMemory(list(data.frame(seconds = 1, host = "a", pid = 1L)), runName = "x", id = "x_1"))
  testthat::local_mocked_bindings(.fitMemoryFile = function(...) stop("disk full"))
  expect_warning(clusters:::.recordFitMemory(list(mkMem("a", 1L, 1)), runName = "x", id = "x_2"), "disk full")
})

test_that("an old ledger without memGB still reads, and takes new rows", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.keepFreeCores = 0)
  old <- data.frame(id = "old", pid = Sys.getpid(), host = "a", workers = 3L, created = Sys.time(),
                    stringsAsFactors = FALSE)
  saveRDS(old, reservationsPath())
  expect_equal(NROW(liveReservations()), 1L)
  nodes <- memNodes(avail = 51)
  expect_equal(capacity(nodes, memPerWorkerGB = 4)$free_est, 10)   # the old row books no memory
  reserveCores(data.frame(host = "a", assign = 2L), memPerWorkerGB = 4)
  expect_equal(liveReservations()$memGB, c(NA, 8))
  expect_equal(capacity(nodes, memPerWorkerGB = 4)$free_est, 8)
  clusters:::.rebookCores("old", data.frame(host = "a", assign = 1L), memPerWorkerGB = 4)
  expect_equal(sort(liveReservations()$memGB), c(4, 8))
})

test_that("workers lost to memory give a smaller cluster, not a wait or an error", {
  withr::local_options(clusters.keepFreeCores = 0, clusters.reservationsPath = withr::local_tempdir())
  nodes <- capacity(memNodes(avail = c(30, 200), free = c(40, 10)), memPerWorkerGB = 4)
  expect_equal(nodes$ram_lost, c(35, 0))
  alloc <- suppressMessages(clusters:::.allocateWhenAvailable(function() nodes, total = 50, waitSeconds = 0))
  expect_equal(sum(alloc$assign), 15L)
  ## a shortage of cores as well as memory still stops
  expect_error(suppressMessages(clusters:::.allocateWhenAvailable(function() nodes, total = 80, waitSeconds = 0)),
               "could only get")
})

test_that("each evaluation records its worker's peak memory", {
  skip_if_not(file.exists("/proc/self/status"))
  expect_gt(clusters:::.peakRssGB(), 0.01)
  expect_true(is.na(clusters:::.procMemoryGB("VmHWM", "/no/such/file")))
  memo <- clusters:::.memoObjFun(function(par) sum(par), character(0), numeric(0))
  clusters:::.deoptimRecordTake()
  memo(c(1, 2))
  expect_gt(clusters:::.deoptimRecordTake()$rss, 0.01)
})

test_that("DEoptimIterative() writes fitMemory.rds, one row per computed chunk", {
  skip_on_cran()
  skip_if_not_installed("DEoptim")
  skip_if_not(file.exists("/proc/self/status"))
  d <- withr::local_tempdir()
  cachePath <- withr::local_tempdir()
  withr::local_options(clusters.reservationsPath = d, reproducible.cachePath = cachePath)
  cl <- localTestCluster(2)
  fn <- function(par) sum((par - 0.3)^2)
  suppressWarnings(suppressMessages(
    clusters:::DEoptimIterative(fn, lower = c(a = 0, b = 0), upper = c(a = 1, b = 1),
                                control = list(NP = 8L, itermax = 2L, trace = FALSE, cluster = cl),
                                figurePath = FALSE, progressFile = FALSE, .plots = NULL, cachePath = cachePath,
                                runName = "fm", .verbose = -1)))
  got <- readRDS(file.path(d, "fitMemory.rds"))
  expect_equal(got$id, c("fm_1", "fm_2"))
  expect_true(all(got$runName == "fm" & got$workers == 2L & got$memMaxGB > 0.01))
})
