## FireSense held-out fits, 2026-10-02: hosts with 48 threads carried 50-52 booked workers, and the master
## host (80 threads, load ~5 for hours) carried 2 of 520. Each cluster was sized once, from the load when it
## was built, and the load average of a host running DEoptim workers is below the workers booked there.
## These tests cover the cap on booked workers, the rule a running cluster uses to decide where its workers
## belong (the same as a new build's), and the move itself, on local workers (no ssh).

withLedger <- function(env = parent.frame()) {
  d <- withr::local_tempdir(.local_envir = env)
  withr::local_options(clusters.reservationsPath = d, .local_envir = env)
  d
}

test_that("a host never shows more free cores than its threads less every worker booked on it", {
  withLedger()
  ## 50 workers booked an hour ago: the load average shows only 30-35 of them, so freeCores() says 13
  ## free; the decay counts the old booking as absorbed and subtracts almost nothing
  reserveCores(data.frame(host = "dougfir", assign = 50L))
  file <- reservationsPath()
  res <- readRDS(file); res$created <- Sys.time() - 3600; saveRDS(res, file)
  nodes <- data.frame(host = c("dougfir", "mega"), cores_total = c(48L, 80L), free_est = c(13, 70))
  out <- freeCoresLessReserved(nodes)
  expect_equal(out$free_est, c(0, 70))
  expect_equal(out$reserved, c(50L, 0L))
})

test_that("a cluster asking about itself does not count its own reservation, and gets its workers back", {
  withLedger()
  mine <- reserveCores(data.frame(host = c("a", "b"), assign = c(6L, 2L)))
  reserveCores(data.frame(host = "a", assign = 2L))
  file <- reservationsPath()
  res <- readRDS(file); res$created <- Sys.time() - 3600; saveRDS(res, file)
  nodes <- data.frame(host = c("a", "b"), nodename = c("a", "b"), cores_total = c(8L, 8L), free_est = c(0, 4))
  fitCapacity <- getFromNamespace(".fitCapacity", "clusters")
  ## a new build: everything booked counts
  built <- suppressMessages(fitCapacity(nodes, total = 8, load_memory = "5min", hostSpeeds = NULL))
  expect_equal(built$free_est, c(0, 4))
  ## the cluster holding `mine`: its 6 + 2 workers are added back and only the other booking caps "a"
  own <- table(c(rep("a", 6), rep("b", 2)))
  asked <- suppressMessages(fitCapacity(nodes, total = 8, load_memory = "5min", hostSpeeds = NULL,
                                        own = own, ownId = mine))
  expect_equal(asked$free_est, c(6, 6))
  expect_equal(asked$reserved, c(2L, 0L))
})

test_that("workers move from the hosts over their target to the hosts short of it, as many as there is room", {
  moves <- getFromNamespace(".rebalanceMoves", "clusters")
  hosts <- c(rep("dougfir", 5), rep("mpb", 3), "mega")
  m <- moves(hosts, c(dougfir = 2L, mpb = 3L, mega = 4L))
  expect_identical(m$remove, 3:5)
  expect_identical(m$add, rep("mega", 3))
  ## nothing to do
  expect_length(moves(hosts, c(dougfir = 5L, mpb = 3L, mega = 1L))$remove, 0L)
  ## room for only one: one moves, from the host furthest over
  m <- moves(hosts, c(dougfir = 3L, mpb = 2L, mega = 2L))
  expect_identical(m$add, "mega")
  expect_identical(m$remove, 5L)
  ## a host the target does not name loses all its workers, if there is room for them
  m <- moves(c("coco", "coco", "a"), c(a = 3L))
  expect_identical(m$remove, 1:2)
  expect_identical(m$add, c("a", "a"))
})

test_that("re-booking moves a reservation and dates the hosts that gained workers", {
  withLedger()
  rebook <- getFromNamespace(".rebookCores", "clusters")
  id <- reserveCores(data.frame(host = c("a", "b"), assign = c(6L, 2L)))
  file <- reservationsPath()
  res <- readRDS(file); res$created <- as.POSIXct("2026-10-02 05:00:00"); saveRDS(res, file)
  other <- reserveCores(data.frame(host = "a", assign = 1L))
  rebook(id, data.frame(host = c("a", "b", "c"), assign = c(3L, 2L, 3L)))
  res <- liveReservations()
  mine <- res[res$id == id, ]
  expect_equal(mine$workers[match(c("a", "b", "c"), mine$host)], c(3L, 2L, 3L))
  expect_equal(mine$created[mine$host == "b"], as.POSIXct("2026-10-02 05:00:00"))
  expect_gt(as.numeric(mine$created[mine$host == "c"]), as.numeric(as.POSIXct("2026-10-02 06:00:00")))
  expect_equal(res$workers[res$id == other], 1L)
})

## Local workers stand in for hosts: each node's `host` label is what decides where it "is".
skip_on_cran()
skip_on_os("windows")
skip_if(nzchar(tolower(Sys.getenv("_R_CHECK_LIMIT_CORES_"))) &&
          !identical(tolower(Sys.getenv("_R_CHECK_LIMIT_CORES_")), "false"),
        "R CMD check limits child processes to 2")

labelled <- function(hosts) {
  cl <- parallel::makeCluster(length(hosts))
  for (i in seq_along(hosts)) cl[[i]]$host <- hosts[i]
  cl
}
fakePlan <- function(free, started) list(
  beta = 0.5,
  startProbe = function() NULL,
  capacity = function(probe, own = NULL, ownId = NULL)
    data.frame(host = names(free), cores_total = 8L, free_est = as.numeric(free) +
                 ifelse(is.na(as.numeric(own[names(free)])), 0, as.numeric(own[names(free)]))),
  startNodes = function(workers, autoStop = FALSE) {
    started$hosts <- c(started$hosts, workers)
    labelled(workers)
  })

test_that("a running cluster moves its workers off a full host, keeps working, and re-books them", {
  withLedger()
  withr::local_options(clusters.rebalanceMinMoves = 1)
  testthat::local_mocked_bindings(.shipToWorkers = function(cl, ...) invisible(cl), .package = "clusters")
  cl <- labelled(c("full", "full", "full", "free"))
  oldPids <- unlist(parallel::clusterCall(cl, Sys.getpid))
  id <- reserveCores(data.frame(host = c("full", "free"), assign = c(3L, 1L)))
  token <- new.env(); token$id <- id
  attr(cl, "reservationToken") <- token
  started <- new.env()
  rebalance <- getFromNamespace(".rebalanceFn", "clusters")(fakePlan(c(full = -2, free = 5), started),
                                                            pkgsNeeded = character(0), objsNeeded = character(0),
                                                            envir = environment(), digest = NULL)
  ## "full" has room for 1 of this cluster's 3 workers, "free" for 6: the even split is 1 and 3
  out <- suppressMessages(rebalance(cl))
  withr::defer(parallel::stopCluster(out))
  hosts <- vapply(out, function(n) n$host, character(1))
  expect_equal(sort(hosts), c("free", "free", "free", "full"))
  expect_identical(started$hosts, c("free", "free"))
  expect_equal(unlist(parallel::parLapply(out, 1:8, function(i) i * 2)), (1:8) * 2)
  newPids <- unlist(parallel::clusterCall(out, Sys.getpid))
  expect_false(any(oldPids[2:3] %in% newPids))
  expect_true(all(c(oldPids[1], oldPids[4]) %in% newPids))
  res <- liveReservations()
  expect_equal(sort(res$workers[res$id == id & res$host == "free"]), 3L)
  expect_equal(res$workers[res$id == id & res$host == "full"], 1L)
  expect_true(is.function(attr(out, "restartCluster")))
})

test_that("nothing moves for fewer than the minimum, and a failed move leaves the cluster and its booking", {
  withLedger()
  testthat::local_mocked_bindings(.shipToWorkers = function(cl, ...) invisible(cl), .package = "clusters")
  cl <- labelled(c("full", "full", "free", "free"))
  withr::defer(parallel::stopCluster(cl))
  id <- reserveCores(data.frame(host = c("full", "free"), assign = c(2L, 2L)))
  token <- new.env(); token$id <- id
  attr(cl, "reservationToken") <- token
  started <- new.env()
  mk <- getFromNamespace(".rebalanceFn", "clusters")
  ## one worker would move; the default minimum for 4 workers is 1, so ask for 2
  withr::local_options(clusters.rebalanceMinMoves = 2)
  rebalance <- mk(fakePlan(c(full = -1, free = 4), started), character(0), character(0), environment(), NULL)
  out <- suppressMessages(rebalance(cl))
  expect_identical(out, cl)
  expect_null(started$hosts)
  ## starting the new workers fails
  withr::local_options(clusters.rebalanceMinMoves = 1)
  plan <- fakePlan(c(full = -1, free = 4), started)
  plan$startNodes <- function(workers, autoStop = FALSE) stop("no route to ", workers[1])
  rebalance <- mk(plan, character(0), character(0), environment(), NULL)
  expect_warning(out <- suppressMessages(rebalance(cl)), "could not move workers")
  expect_identical(out, cl)
  expect_equal(unlist(parallel::clusterCall(out, function() 1L)), rep(1L, 4))
  res <- liveReservations()
  expect_equal(res$workers[res$id == id][order(res$host[res$id == id])], c(2L, 2L))
})

test_that("DEoptimIterative() asks the cluster to rebalance every clusters.rebalanceEvery generations", {
  skip_if_not_installed("DEoptim")
  skip_if(identical(Sys.getenv("NOT_CRAN"), ""), "PSOCK workers need the installed package")
  skip_if(isTRUE(parallel::detectCores() < 4L), "needs 4 cores")
  cl <- localTestCluster(4L)
  calls <- new.env(); calls$at <- integer(0)
  attr(cl, "rebalance") <- function(cl) { calls$at <- c(calls$at, length(calls$at) + 1L); cl }
  withr::local_options(reproducible.cachePath = withr::local_tempdir(), reproducible.useCache = FALSE,
                       clusters.rebalanceEvery = 2L)
  DE <- suppressWarnings(suppressMessages(clusters:::DEoptimIterative(
    function(par) sum((par - 0.3)^2), lower = c(a = 0, b = 0), upper = c(a = 1, b = 1),
    control = list(NP = 4L, strategy = 2L, itermax = 5, trace = FALSE, cluster = cl),
    figurePath = FALSE, .plots = NULL, runName = "rb", .verbose = -1)))
  ## after generations 2 and 4; not after 5, the last
  expect_length(calls$at, 2L)
  expect_length(DE, 5L)
  withr::local_options(clusters.rebalanceEvery = 0L)
  calls$at <- integer(0)
  DE <- suppressWarnings(suppressMessages(clusters:::DEoptimIterative(
    function(par) sum((par - 0.4)^2), lower = c(a = 0, b = 0), upper = c(a = 1, b = 1),
    control = list(NP = 4L, strategy = 2L, itermax = 3, trace = FALSE, cluster = cl),
    figurePath = FALSE, .plots = NULL, runName = "rb0", .verbose = -1)))
  expect_length(calls$at, 0L)
})
