## Where a cluster's workers go, 2026-10-13. Hard caps on every host, whatever the speed model says: all
## workers and other load within the threads less clusters.keepFreeCores, and memory within 0.9 of the
## host's; and the speed model corrected by what the fits measured (pinus: 8 cores on 3 memory channels
## ran 1.45x the generation median where the model predicted 0.87 of it).

## withLedger() and node() are in helper-placement.R

capacity <- function(nodes, total = 40, ...)
  suppressMessages(clusters:::.fitCapacity(nodes, total = total, load_memory = "5min", ...))

test_that("a host never gets more workers than its threads less the kept cores less every load on it", {
  withLedger()
  ## 10 of other load that no cluster booked: freeCores(fraction = 0.9) still says 5 free, and 5 more
  ## workers would make 15 on 16 threads
  n <- capacity(node("pinus", load = 10))
  expect_equal(n$free_est, 4)
  a <- clusters:::.speedAllocate(n, total = 10, beta = 0.75)
  expect_equal(a$assign + a$busy, 14)
  ## with reservations off the cap still holds
  withr::local_options(clusters.useReservations = FALSE)
  expect_equal(capacity(node("pinus", load = 10))$free_est, 4)
  ## the kept cores are an option
  withr::local_options(clusters.useReservations = TRUE, clusters.keepFreeCores = 4)
  expect_equal(capacity(node("pinus", load = 10))$free_est, 2)
  ## booked workers count the same way, and the larger of the two counts: 12 booked an hour ago (so
  ## absorbed into the load average, which shows 10 of them) with 5 free by freeCores()
  withr::local_options(clusters.keepFreeCores = 2)
  reserveCores(data.frame(host = "pinus", assign = 12L))
  res <- readRDS(reservationsPath()); res$created <- Sys.time() - 3600; saveRDS(res, reservationsPath())
  n <- capacity(node("pinus", load = 10, free = 5))
  expect_equal(n$busy, 12)
  expect_equal(n$free_est, 2)
})

test_that("memory used plus the new workers' memory stays within 0.9 of the host's memory", {
  withLedger()
  set.seed(2)
  for (i in 1:50) {
    total <- sample(c(94, 128, 256, 1024), 1)
    avail <- runif(1, 0, total)
    mem <- sample(c(4, 6, 14), 1)
    n <- capacity(node("h", avail = avail, total = total), memPerWorkerGB = mem)
    a <- clusters:::.speedAllocate(n, total = 100, beta = 0.75)
    ## a host already past 0.9 of its memory gets none
    if (a$assign > 0) expect_lte((total - avail) + a$assign * mem, 0.9 * total + 1e-9)
    ## and one more worker would not have fitted, unless cores ran out first
    if (a$assign < n$cores_total - 2) expect_gt((total - avail) + (a$assign + 1) * mem, 0.9 * total)
  }
})

## two chunks' records for identical hosts: one measured 1.45x the median, the other 0.93x
speeds <- function(ratios, workers = 2L, n = 100L, time = Sys.time(), ids = c("f_1", "f_2"))
  do.call(rbind, lapply(ids, function(id)
    data.frame(host = names(ratios), n = n, median = ratios, p90 = ratios * 1.3, ratio = unname(ratios),
               time = time, id = id, workers = workers, stringsAsFactors = FALSE)))

test_that("a host that measured slower than the model says gets fewer workers than an identical host", {
  withLedger()
  nodes <- rbind(node("pinus"), node("abies"))
  n <- capacity(nodes, speeds = speeds(c(pinus = 1.45, abies = 0.93)))
  expect_lt(n$speedFactor[1], n$speedFactor[2])
  expect_lt(n$speedFactor[1], 1); expect_gt(n$speedFactor[2], 1)
  a <- clusters:::.speedAllocate(n, total = 10, beta = 0.75)
  expect_lt(a$assign[1], a$assign[2])
  ## the slow host is still used when that is fastest overall: past abies' 8 cores, pinus' first workers
  ## (speed 0.83 or so) beat abies' ninth (shared cores)
  a <- clusters:::.speedAllocate(n, total = 20, beta = 0.75)
  expect_gt(a$assign[1], 0)
  expect_equal(sum(a$assign), 20)
  ## the factor is in the reported speed
  expect_equal(a$speed, round(n$speedFactor * clusters:::.workerSpeed(a$assign, 8, 8, 16, 0.75), 3))
})

test_that("without measurements placement is the hardware model alone", {
  withLedger()
  nodes <- rbind(node("a"), node("b", load = 3))
  n <- capacity(nodes)                       # no hostSpeed.rds
  expect_equal(n$speedFactor, c(1, 1))
  expect_equal(capacity(nodes, speeds = NULL)$speedFactor, c(1, 1))
  expect_equal(capacity(nodes, speeds = data.frame())$speedFactor, c(1, 1))
  ## a host with no record keeps 1 while the others are corrected
  n <- capacity(nodes, speeds = speeds(c(b = 1.2)))
  expect_equal(n$speedFactor[1], 1)
  expect_lt(n$speedFactor[2], 1)
  ## the same allocation as with no speedFactor column at all
  a0 <- clusters:::.speedAllocate(capacity(nodes)[setdiff(names(n), "speedFactor")], total = 12, beta = 0.75)
  expect_equal(clusters:::.speedAllocate(capacity(nodes), total = 12, beta = 0.75)$assign, a0$assign)
})

test_that("the measured correction compares with the model at the workers recorded, shrinks by evaluations and age, and reads old rows", {
  factor <- clusters:::.hostSpeedFactor
  nodes <- rbind(node("a"), node("b"))
  ## both hosts at full speed in the model (2 workers): the ratio alone is the correction
  s <- speeds(c(a = 2, b = 0.5), n = 1e6)
  expect_equal(factor(nodes, s, priorN = 0), c(0.5, 2))
  ## a host whose 14 booked workers the model already expects to run slower: measuring that is no fault,
  ## whatever this fit's own share of them
  s <- speeds(c(a = 1 / clusters:::.workerSpeed(14, 8, 8, 16, 0.75), b = 1), workers = 2L, n = 1e6)
  s$booked <- c(14L, 2L)
  expect_equal(factor(nodes, s, priorN = 0), c(1, 1), tolerance = 1e-3)
  ## and a host that ran faster than the model expects at its load is credited
  s$ratio[s$host == "a"] <- 1
  expect_gt(factor(nodes, s, priorN = 0)[1], 1.1)
  ## without `booked` the fit's own workers stand in
  s$booked <- NA; s$workers <- c(14L, 2L); s$ratio[s$host == "a"] <- 1 / clusters:::.workerSpeed(14, 8, 8, 16, 0.75)
  expect_equal(factor(nodes, s, priorN = 0), c(1, 1), tolerance = 1e-3)
  ## one short record moves a host little: 4 evaluations at ratio 3 with a prior of 100
  s <- speeds(c(a = 3), n = 4L, ids = "f_1")
  expect_equal(factor(nodes, s, priorN = 100), c(exp(-log(3) * 4 / 104), 1))
  ## last week's rows count less than today's (a was slow then, fast now)
  s <- rbind(speeds(c(a = 2), time = Sys.time() - 7 * 86400, ids = "old_1"),
             speeds(c(a = 0.5), ids = "new_1"))
  expect_gt(factor(nodes, s, priorN = 0, halfLifeDays = 1)[1], 1.9)
  expect_equal(factor(nodes, s, priorN = 0, halfLifeDays = Inf)[1], 1)
  ## rows older than the window do not count at all
  expect_equal(factor(nodes, speeds(c(a = 2), time = Sys.time() - 40 * 86400), days = 30), c(1, 1))
  ## rows written before `workers` existed (pre-0.0.65) are read, as a ratio against the model at 1
  old <- speeds(c(a = 2))[setdiff(names(s), "workers")]
  expect_equal(factor(nodes, old, priorN = 0), c(0.5, 1))
  ## and so are rows with NA workers, hosts not in `nodes`, and unusable ratios
  s <- speeds(c(a = 2, zzz = 5)); s$workers <- NA
  expect_equal(factor(nodes, s, priorN = 0), c(0.5, 1))
  s <- speeds(c(a = NaN, b = 0))
  expect_equal(factor(nodes, s), c(1, 1))
  ## the real file is what .fitCapacity() reads
  withLedger()
  saveRDS(speeds(c(a = 2)), clusters:::.hostSpeedFile())
  expect_equal(capacity(nodes, beta = 0.75)$speedFactor, factor(nodes, speeds(c(a = 2))))
})

test_that("a running cluster asking where its workers belong obeys the same caps", {
  withLedger()
  ## 16-thread hosts: `a` carries this cluster's 6 workers and 10 of other load, `b` is idle. The 6
  ## workers come off the load and the cap leaves room for 4 on `a`
  nodes <- rbind(node("a", load = 16, free = 1), node("b"))
  n <- capacity(nodes, own = c(a = 6))
  expect_equal(n$busy, c(10, 0))
  expect_equal(n$free_est, c(4, 14))
  target <- clusters:::.speedAllocate(n, total = 6, beta = 0.75)
  expect_lte(target$assign[1] + target$busy[1], 14)
  m <- clusters:::.rebalanceMoves(rep("a", 6), stats::setNames(target$assign, target$host))
  expect_gte(length(m$remove), 2L)
  ## and memory: `a` has room for 2 workers' memory beyond its own
  n <- capacity(rbind(node("a", avail = 10, total = 100), node("b")), own = c(a = 6), memPerWorkerGB = 4)
  expect_equal(n$free_est[1], floor((10 + 6 * 4 - 10) / 4))
})

test_that("the cluster that .rebalanceFn() moves is sized by .fitCapacity()'s caps", {
  skip_on_cran()
  skip_on_os("windows")
  skip_if(nzchar(tolower(Sys.getenv("_R_CHECK_LIMIT_CORES_"))) &&
            !identical(tolower(Sys.getenv("_R_CHECK_LIMIT_CORES_")), "false"),
          "R CMD check limits child processes to 2")
  withLedger()
  withr::local_options(clusters.rebalanceMinMoves = 1)
  testthat::local_mocked_bindings(.shipToWorkers = function(cl, ...) invisible(cl), .package = "clusters")
  labelled <- function(hosts) {
    cl <- parallel::makeCluster(length(hosts))
    for (i in seq_along(hosts)) cl[[i]]$host <- hosts[i]
    cl
  }
  cl <- labelled(rep("a", 4))
  id <- reserveCores(data.frame(host = "a", assign = 4L))
  token <- new.env(); token$id <- id
  attr(cl, "reservationToken") <- token
  ## `a`: 16 threads, this cluster's 4 workers plus 12 of other load; `b` idle
  nodes <- rbind(node("a", load = 16, free = 1), node("b"))
  plan <- list(beta = 0.75, startProbe = function() NULL, memPerWorkerGB = function() NA_real_,
               capacity = function(probe, own = NULL, ownId = NULL)
                 capacity(nodes, total = 4, own = own, ownId = ownId),
               startNodes = function(workers, autoStop = FALSE) labelled(workers))
  rebalance <- clusters:::.rebalanceFn(plan, character(0), character(0), environment(), NULL)
  out <- suppressMessages(rebalance(cl))
  withr::defer(parallel::stopCluster(out))
  hosts <- vapply(out, function(n) n$host, character(1))
  expect_lte(sum(hosts == "a"), 2L)   # 16 - 2 kept - 12 other load
  expect_equal(sum(hosts == "b"), 4L - sum(hosts == "a"))
  res <- liveReservations()
  expect_equal(sum(res$workers[res$id == id & res$host == "a"]), sum(hosts == "a"))
  ## when the caps hold fewer workers than the cluster has, it keeps them all: a rebalance never shrinks a
  ## running cluster (its NP is its workers)
  nodes <- rbind(node("a", load = 16, free = 1), node("b", load = 13, free = 2))   # room for 2 + 1
  out2 <- suppressMessages(rebalance(out))
  withr::defer(if (!identical(out2, out)) parallel::stopCluster(out2))
  expect_length(out2, 4L)
  expect_equal(sum(liveReservations()$workers[liveReservations()$id == id]), 4L)
})

test_that("a build waits for memory as it waits for cores: 40 asked for, 2 fit, it waits for 40", {
  withLedger()
  ## memory leaves 2 workers of the 40 asked for: it does not start with the 2 (FireSense fit 12.3,
  ## 2026-10-12: it did, and died in .clusterNP() seven minutes later)
  two <- capacity(node("a", avail = 20, total = 100, threads = 48L), memPerWorkerGB = 4)
  expect_equal(two$ram_lost, 43 - 2)
  expect_error(suppressMessages(clusters:::.allocateWhenAvailable(function() two, total = 40, waitSeconds = 0)),
               "could only get 2 of 40 workers.*free memory kept workers off: a=41 \\(20 GB free\\)")
  ## it keeps probing, with a mocked clock, until all 40 fit, and never returns the 2
  forty <- capacity(node("a", avail = 200, total = 300, threads = 48L), memPerWorkerGB = 4)
  t0 <- as.POSIXct("2026-10-13 09:00:00")
  calls <- 0; clock <- t0
  probe <- function() { calls <<- calls + 1; if (calls < 5) two else forty }
  alloc <- suppressMessages(clusters:::.allocateWhenAvailable(
    probe, total = 40, waitSeconds = 3600, sleep = function(s) clock <<- clock + s, now = function() clock))
  expect_equal(sum(alloc$assign), 40L)
  expect_equal(calls, 5)
  expect_equal(as.numeric(clock - t0, units = "secs"), 4 * 60)
  ## and stops at the deadline with the shortfall, cores or memory, never with fewer workers
  calls <- 0; clock <- t0
  expect_error(suppressMessages(clusters:::.allocateWhenAvailable(
    function() { calls <<- calls + 1; two }, total = 40, waitSeconds = 150,
    sleep = function(s) clock <<- clock + s, now = function() clock)),
    "could only get 2 of 40 workers after waiting 3 min")
  expect_equal(calls, 4)
  ## clusters.minWorkersFraction is the explicit opt-in for a partial start
  alloc <- suppressMessages(clusters:::.allocateWhenAvailable(function() two, total = 40, minFraction = 0.05,
                                                              waitSeconds = 0))
  expect_equal(sum(alloc$assign), 2L)
})
