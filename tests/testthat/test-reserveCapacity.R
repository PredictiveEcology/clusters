## Jobs that are not PSOCK workers book cores and memory too, 2026-10-13. A FireSense prediction (one
## SpaDES simulation, about 38 GB and 1-2 cores, a tmux pane of experimentTmux()) was invisible to the
## ledger that places a fit's workers, so fits and predictions overbooked a host. reserveCapacity() books a
## job in the same ledger, under the same caps, waiting as a build waits.

capacity <- function(nodes, ...)
  suppressMessages(clusters:::.fitCapacity(nodes, total = 40, load_memory = "5min", ...))

reserve <- function(..., clk = NULL)
  suppressMessages(if (is.null(clk)) reserveCapacity(...) else reserveCapacity(..., sleep = clk$sleep, now = clk$now))

test_that("a job's booking is what a fit's placement then sees on that host", {
  withLedger()
  ## an 80-thread host with 150 of 187 GB available: a fit of 14 GB workers would get 9 there
  mega <- node("mega", threads = 80L, avail = 150, total = 187)
  before <- capacity(mega, memPerWorkerGB = 14)
  expect_equal(before$free_est, 9)
  token <- reserve(2, 38, hosts = "mega", probe = function() mega)
  expect_true(is.environment(token))
  expect_equal(token$host, "mega")
  res <- liveReservations()
  expect_equal(res$id, token$id)
  expect_equal(res$pid, Sys.getpid())
  expect_equal(res$workers, 2L)
  expect_equal(res$memGB, 38)
  ## cores: 2 fewer (72 free by freeCores(), less the 2); memory: 38 GB less, so 6 workers of 14 GB, not 9
  expect_equal(capacity(mega)$free_est, 70)
  after <- capacity(mega, memPerWorkerGB = 14)
  expect_equal(after$reserved, 2L)
  expect_equal(after$reserved_gb, 38)
  expect_equal(after$free_est, floor((150 - 38 - 0.1 * 187) / 14))
  expect_equal(clusters:::.speedAllocate(after, total = 100, beta = 0.75)$assign, 6L)
  ## 1.2 cores is booked as 2: the ledger holds whole cores
  token2 <- reserve(1.2, 10, hosts = "mega", probe = function() mega)
  expect_equal(token2$cores, 2L)
  expect_equal(sum(liveReservations()$workers), 4L)
})

test_that("a job that does not fit waits for room, and at the deadline stops with the shortfall", {
  withLedger()
  ## 16 threads with 13 of load: 1 core within the cap (16 - 2 kept - 13), the job needs 2
  full <- node("a", load = 13)
  clk <- fakeClock()
  expect_error(reserve(2, 4, hosts = "a", probe = function() full, waitSeconds = 120, clk = clk),
               "could only get 0 of 2 cores after waiting 2 min")
  expect_equal(clk$sleeps(), 2L)
  expect_equal(NROW(liveReservations()), 0L)
  ## memory short, not cores: 40 of 128 GB available, 12.8 kept, 27.2 for a job of 38
  tight <- node("a", avail = 40, total = 128)
  expect_error(reserve(2, 38, hosts = "a", probe = function() tight, waitSeconds = 0),
               "could only get 0 of 2 cores.*free memory kept cores off: a=.* \\(40 GB free\\)")
  ## a job larger than the host could ever hold stops at once
  expect_error(reserve(20, 4, hosts = "a", probe = function() node("a"), waitSeconds = 3600, clk = clk),
               "more cores than")
  ## it keeps probing until the job fits, then books it
  calls <- 0
  probe <- function() { calls <<- calls + 1; if (calls < 3) full else node("a") }
  clk <- fakeClock()
  token <- reserve(2, 4, hosts = "a", probe = probe, waitSeconds = 3600, interval = 30, clk = clk)
  expect_equal(calls, 3)
  expect_equal(clk$sleeps(), 2L)
  expect_equal(as.numeric(clk$now() - fakeClock()$now(), units = "secs"), 60)
  expect_equal(liveReservations()$workers, 2L)
})

test_that("the memory cap is the fit's: used plus the job's memory within 0.9 of the host's", {
  withLedger()
  ## 128 GB host keeps 12.8 GB: 50 GB available leaves 37.2 for a 38 GB job, 51 leaves 38.2
  expect_error(reserve(1, 38, hosts = "a", probe = function() node("a", avail = 50, total = 128)),
               "could only get 0 of 1 cores")
  token <- reserve(1, 38, hosts = "a", probe = function() node("a", avail = 51, total = 128))
  expect_equal(liveReservations()$memGB, 38)
  ## with the job booked a second one of the same size does not fit: its memory is booked in full
  expect_error(reserve(1, 38, hosts = "a", probe = function() node("a", avail = 51, total = 128)),
               "could only get 0 of 1 cores")
})

test_that("a booking is freed by release, by the wrapper, and when its process dies", {
  withLedger()
  a <- node("a")
  token <- reserve(2, 38, hosts = "a", probe = function() a)
  expect_equal(NROW(liveReservations()), 1L)
  releaseCapacity(token)
  expect_equal(NROW(liveReservations()), 0L)
  expect_equal(capacity(a)$reserved, 0L)
  ## the wrapper books around its code and releases after it, also when the code fails
  inside <- suppressMessages(withCapacity(2, 38, liveReservations(), hosts = "a", probe = function() a))
  expect_equal(inside$workers, 2L)
  expect_equal(NROW(liveReservations()), 0L)
  expect_error(suppressMessages(withCapacity(2, 38, stop("boom"), hosts = "a", probe = function() a)), "boom")
  expect_equal(NROW(liveReservations()), 0L)
  ## a booking whose process is gone is dropped on the next read, as a cluster's is
  token <- reserve(2, 38, hosts = "a", probe = function() a)
  res <- readRDS(reservationsPath()); res$pid <- 999999L; saveRDS(res, reservationsPath())
  expect_equal(NROW(liveReservations()), 0L)
  expect_equal(capacity(a, memPerWorkerGB = 14)$reserved_gb, 0)
})

test_that("with several candidate hosts the job goes to the first where it fits now", {
  withLedger()
  nodes <- rbind(node("a", load = 13), node("b"), node("c"))
  token <- reserve(2, 4, hosts = c("a", "b", "c"), probe = function() nodes)
  expect_equal(token$host, "b")
  res <- liveReservations()
  expect_equal(res$host, "b")
  expect_equal(res$workers, 2L)
})

test_that("without a probe the job is booked on this host, measured in this process", {
  withLedger()
  ## the local measurement is the one the probe cluster makes on every host
  cap <- clusters:::.probeHost("5min", 0.9)
  here <- clusters:::.nodeTable("here", list(cap))
  expect_equal(NROW(here), 1L)
  expect_true(all(c("host", "nodename", "cores_total", "cores_physical", "dimms", "free_est", "loadavg_5",
                    "mem_available_gb", "mem_total_gb") %in% names(here)))
  expect_equal(here$host, "here")
  expect_equal(here$nodename, Sys.info()[["nodename"]])
  expect_gte(here$cores_total, 1L)
  ## the default host is "localhost": what every build running on this machine calls it in the ledger
  ## (plan_psock_min() renames the machine's own SSH alias); it is this machine's, so it counts against the
  ## machine under its nodename too
  token <- reserve(1, 0, probe = function() node("localhost"))
  expect_equal(token$host, "localhost")
  expect_equal(liveReservations()$host, "localhost")
  expect_equal(capacity(node("localhost"))$reserved, 1L)
  expect_equal(capacity(node(Sys.info()[["nodename"]]))$reserved, 1L)
  releaseCapacity(token)
  ## a job on another host needs a probe that can see it
  expect_error(reserve(1, 0, hosts = "elsewhere"), "probe")
  ## a job this host could never hold stops at once, the default probe having measured it
  expect_error(reserve(here$cores_total + 1L, 0), "more cores than")
})

test_that("a booking whose token was not kept survives garbage collection until released", {
  withLedger()
  a <- node("a")
  reserve(2, 38, hosts = "a", probe = function() a)
  gc()
  expect_equal(NROW(liveReservations()), 1L)
  id <- liveReservations()$id
  releaseCapacity(get(id, envir = clusters:::.capacityTokens))
  expect_equal(NROW(liveReservations()), 0L)
  expect_false(exists(id, envir = clusters:::.capacityTokens))
})
