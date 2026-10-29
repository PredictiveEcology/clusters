## 2026-10-09: placement booked only cores. Host `core` (187 GB) was filled to 187.1 GB by DEoptim workers and
## hung for 1.5 h, killing every fit with a worker there. A host now gets only as many workers as its free
## memory holds, from what the same fit used last time (fitMemory.rds), else clusters.workerMemoryGB.

memNodes <- function(avail, total = 100, free = 40, host = letters[seq_along(avail)], cores = 48L)
  data.frame(host = host, cores_total = cores, free_est = free, loadavg_5 = 0,
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
  expect_true(is.na(clusters:::.memPerWorkerGB("elf1")))   # no file, no option: no memory cap
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 5))), runName = "elf1", id = "elf1_1")
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 9))), runName = "elf2", id = "elf2_1")
  expect_equal(clusters:::.memPerWorkerGB("elf1"), c(median = 2, max = 5))
  file <- clusters:::.fitMemoryFile()
  got <- readRDS(file)
  got$time[got$id == "elf1_1"] <- Sys.time() - 3600
  saveRDS(got, file)
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 3))), runName = "elf1", id = "elf1_2")
  expect_equal(clusters:::.memPerWorkerGB("elf1"), c(median = 2, max = 3))   # the most recent, not the largest
  expect_equal(clusters:::.memPerWorkerGB("elf2"), c(median = 2, max = 9))
  ## no record for this runName: no other runName's figure (no option: no cap)
  expect_true(is.na(clusters:::.memPerWorkerGB("elf3")))
  expect_true(is.na(clusters:::.memPerWorkerGB()))
  ## and it sets the cap: 1 + floor((20 - 9) / 2) = 6
  withr::local_options(clusters.keepFreeCores = 0)
  expect_equal(capacity(memNodes(avail = 30), memPerWorkerGB = clusters:::.memPerWorkerGB("elf2"))$free_est, 6)
})

test_that("with no records at all clusters.workerMemoryGB is used, and with no option nothing is capped", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.keepFreeCores = 0)
  expect_true(is.na(clusters:::.memPerWorkerGB("elf1")))
  nodes <- memNodes(avail = 15, total = 16)   # a small machine
  expect_equal(capacity(nodes, memPerWorkerGB = clusters:::.memPerWorkerGB("elf1"))$free_est, 40)
  withr::local_options(clusters.workerMemoryGB = 14)
  expect_equal(capacity(nodes, memPerWorkerGB = clusters:::.memPerWorkerGB("elf1"))$free_est, 0)
  withr::local_options(clusters.workerMemoryGB = 6)
  expect_equal(clusters:::.memPerWorkerGB("elf1"), 6)
  saveRDS(NULL, clusters:::.fitMemoryFile())   # an unusable file is no record either
  expect_equal(clusters:::.memPerWorkerGB("elf1"), 6)
  unlink(clusters:::.fitMemoryFile())
  ## records older than the window are no record
  clusters:::.recordFitMemory(list(mkMem("h", 1L, 20)), runName = "old", id = "old_1")
  file <- clusters:::.fitMemoryFile()
  got <- readRDS(file); got$time <- Sys.time() - 40 * 86400; saveRDS(got, file)
  expect_equal(clusters:::.memPerWorkerGB("elf1"), 6)
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
  ## re-booked without a memory per worker (workers dropped at a rebuild): each host keeps what it was booked at
  clusters:::.rebookCores("old", data.frame(host = "a", assign = 2L))
  expect_equal(sort(liveReservations()$memGB), c(8, 8))
})

test_that("workers lost to memory are waited for like cores, and the messages name the memory", {
  withr::local_options(clusters.keepFreeCores = 0, clusters.reservationsPath = withr::local_tempdir())
  nodes <- capacity(memNodes(avail = c(30, 200), free = c(40, 10)), memPerWorkerGB = 4)
  expect_equal(nodes$ram_lost, c(35, 0))
  ## 2026-10-13: a build started smaller by the workers memory took (here 15 of 50); it waits now
  expect_error(suppressMessages(clusters:::.allocateWhenAvailable(function() nodes, total = 50, waitSeconds = 0)),
               "could only get 15 of 50 workers.*free memory kept workers off: a=35 \\(30 GB free\\)")
  ## when memory leaves nothing, the error names memory, not just cores
  none <- capacity(memNodes(avail = 5, free = 8), memPerWorkerGB = 4)
  expect_error(suppressMessages(clusters:::.allocateWhenAvailable(function() none, total = 4, waitSeconds = 0)),
               "free memory kept workers off: a=8 \\(5 GB free\\)")
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

test_that("a fit without a run name still records its memory, and the record is found", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.workerMemoryGB = 14)
  ## 2026-10-13: as.character(NULL) made a zero-row frame, nothing was written, and every FireSense
  ## build sized its hosts at the 14 GB option instead of the ~4.5 GB measured
  expect_no_warning(got <- clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 5))), runName = NULL, id = "pid1_1"))
  expect_equal(got$memMaxGB, 5)
  expect_true(is.na(got$runName))
  expect_equal(nrow(readRDS(clusters:::.fitMemoryFile())), 1L)
  expect_equal(clusters:::.memPerWorkerGB("other"), 14)   # another run's record is not used: the option
  expect_no_warning(clusters:::.recordFitMemory(list(mkMem("h", 1L, 2)), runName = "", id = "pid1_2"))
  expect_equal(nrow(readRDS(clusters:::.fitMemoryFile())), 2L)
})

## 2026-10-28: a run's workers were all budgeted at its largest (ELF 6.6.1: median 1.7 GB, one worker 7.9 GB),
## and a run with no record took the largest of any other run's (ELF 9.2.2 at 8.5 GB, from ELF 12.1_2).
test_that("a cluster is budgeted at (n - 1) * median + max, not n * max", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.keepFreeCores = 0)
  clusters:::.recordFitMemory(list(mkMem("h", 1:10, c(rep(1.7, 9), 7.9))), runName = "elf661", id = "elf661_1")
  mem <- clusters:::.memPerWorkerGB("elf661")
  expect_equal(unname(mem), c(1.7, 7.9))
  ## ~100 GB usable (110 free less 10 headroom): 1 + floor((100 - 7.9) / 1.7) = 55 workers, not floor(100 / 7.9) = 12
  nodes <- memNodes(avail = 110, free = 200, cores = 400L)
  expect_equal(capacity(nodes, memPerWorkerGB = mem)$free_est, 55)
  expect_equal(capacity(nodes, memPerWorkerGB = 7.9)$free_est, 12)
  expect_message(clusters:::.fitCapacity(nodes, 80, "5min", memPerWorkerGB = mem), "median 1.7 GB, largest 7.9 GB per worker")
  ## the largest n with (n - 1) * 1.7 + 7.9 <= 100
  expect_lte((55 - 1) * 1.7 + 7.9, 100); expect_gt((56 - 1) * 1.7 + 7.9, 100)
  ## not even the largest worker fits: none
  expect_equal(capacity(memNodes(avail = 15, free = 200, cores = 400L), memPerWorkerGB = mem)$free_est, 0)
})

test_that("a booking uses the same rule, and a rebalancing cluster's own workers are added back by it", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.keepFreeCores = 0)
  id <- reserveCores(data.frame(host = c("a", "b"), assign = c(5L, 1L)), memPerWorkerGB = c(2, 9))
  got <- liveReservations()
  expect_equal(got$memGB, c(4 * 2 + 9, 9))
  expect_equal(names(got), c("id", "pid", "host", "workers", "created", "memGB"))   # the ledger format is unchanged
  clusters:::.rebookCores(id, data.frame(host = "a", assign = 3L), memPerWorkerGB = c(2, 9))
  expect_equal(liveReservations()$memGB, 2 * 2 + 9)
  releaseCores(id = id)
  ## own workers on a host give back what they were budgeted at: 10 + (3 - 1) * 2 + 9 = 23 usable less 10 headroom
  nodes <- memNodes(avail = 10, free = 200, host = "a", cores = 400L)
  expect_equal(capacity(nodes, memPerWorkerGB = c(2, 9), own = c(a = 3))$free_est, 1 + floor((10 + 13 - 10 - 9) / 2))
})

test_that("a run with no record of its own uses the option, or no cap, never another run's figure", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.keepFreeCores = 0)
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 8.5))), runName = "elf1212", id = "elf1212_1")
  expect_true(is.na(clusters:::.memPerWorkerGB("elf922")))
  nodes <- memNodes(avail = 110, free = 200, cores = 400L)
  expect_equal(capacity(nodes, memPerWorkerGB = clusters:::.memPerWorkerGB("elf922"))$free_est, 200)
  withr::local_options(clusters.workerMemoryGB = 5)
  expect_equal(clusters:::.memPerWorkerGB("elf922"), 5)
  expect_equal(capacity(nodes, memPerWorkerGB = clusters:::.memPerWorkerGB("elf922"))$free_est, 20)
})

test_that("a run with a record of its own uses it, whatever the option and the other runs say", {
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(), clusters.workerMemoryGB = 14)
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 2, 8.5))), runName = "elf1212", id = "elf1212_1")
  clusters:::.recordFitMemory(list(mkMem("h", 1:3, c(1, 1, 3))), runName = "elf922", id = "elf922_1")
  expect_equal(clusters:::.memPerWorkerGB("elf922"), c(median = 1, max = 3))
})
