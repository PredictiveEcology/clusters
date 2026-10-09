## A worker that dies without closing its socket used to leave the master blocked in unserialize() for
## parallelly's 30-day socket timeout (FireSense, 2026-09-29: 35+ minutes at 0% CPU, then interrupted by
## hand). A stopped process (SIGSTOP) is the local stand-in: its socket stays open and it never replies.
## These tests use local PSOCK workers only: no ssh, no network.

skip_on_cran()
skip_on_os("windows")
skip_if(nzchar(tolower(Sys.getenv("_R_CHECK_LIMIT_CORES_"))) &&
          !identical(tolower(Sys.getenv("_R_CHECK_LIMIT_CORES_")), "false"),
        "R CMD check limits child processes to 2")

for (f in c(".replaceDeadNodes", ".deadNodes", ".runWithRebuild", ".setWorkerTimeout", ".stopNodes",
            ".stopCluster", ".workerTimeout", ".sshTunnelOpts", ".restartClusterFn", ".nodeHosts"))
  assign(f, getFromNamespace(f, "clusters"))

localCluster <- function(n) {
  cl <- setWorkerLibs(parallel::makeCluster(n))
  attr(cl, "pids") <- unlist(parallel::clusterCall(cl, Sys.getpid))
  cl
}
killAll <- function(pids) for (p in pids) try(tools::pskill(p, tools::SIGKILL), silent = TRUE)

test_that("a node killed after start is replaced at set-up and the cluster answers", {
  cl <- localCluster(3L)
  pids <- attr(cl, "pids")
  withr::defer(killAll(pids))
  tools::pskill(pids[2], tools::SIGKILL)
  Sys.sleep(0.5)
  started <- character(0)
  start <- function(host) {
    started <<- c(started, host)
    new <- localCluster(1L)
    pids <<- c(pids, attr(new, "pids"))
    new
  }
  out <- .replaceDeadNodes(cl, start, seconds = 5, tries = 2L, action = "stop")
  expect_length(out$cluster, 3L)
  expect_identical(out$dropped, integer(0))
  expect_identical(started, "localhost")
  expect_equal(unlist(parallel::clusterCall(out$cluster, function() 1L)), rep(1L, 3L))
  expect_false(pids[2] %in% unlist(parallel::clusterCall(out$cluster, Sys.getpid)))
  parallel::stopCluster(out$cluster)
})

test_that("a node that is alive but never answers is found within the timeout", {
  cl <- localCluster(2L)
  pids <- attr(cl, "pids")
  withr::defer(killAll(pids))
  tools::pskill(pids[1], tools::SIGSTOP)
  took <- system.time(dead <- .deadNodes(cl, seconds = 2))[["elapsed"]]
  expect_identical(dead, 1L)
  expect_lt(took, 20)
  killAll(pids); try(parallel::stopCluster(cl), silent = TRUE)
})

test_that("a node that never answers is found within the timeout while later's input handler is firing", {
  skip_if_not_installed("later")
  ## After a later callback has run outside R's top level (as shiny::testServer() leaves a session),
  ## later's input handler fires every millisecond, and R's socket read never reaches its timeout: a read
  ## from a stopped worker hung for good (2026-10-08). That state cannot be undone, so it is made in a
  ## worker process, which then runs the check on a cluster of its own.
  outer <- localTestCluster(1L)
  socketTimeout(outer[[1]]$con, 60)                     # a hang here is a failure, not a stuck suite
  check <- function(libs) {
    later::later(function() NULL, 0)
    Sys.sleep(0.2)
    later::run_now()
    cl <- parallel::makeCluster(2L)
    parallel::clusterCall(cl, function(libs) { .libPaths(libs); loadNamespace("clusters") }, libs)   # as setWorkerLibs()
    pids <- unlist(parallel::clusterCall(cl, Sys.getpid))
    on.exit(for (p in pids) tools::pskill(p, tools::SIGKILL))
    tools::pskill(pids[1], tools::SIGSTOP)
    deadNodes <- utils::getFromNamespace(".deadNodes", "clusters")
    took <- system.time(dead <- deadNodes(cl, seconds = 2))[["elapsed"]]
    list(dead = dead, took = took)
  }
  environment(check) <- globalenv()
  out <- parallel::clusterCall(outer, check, .libPaths())[[1]]
  expect_identical(out$dead, 1L)
  expect_lt(out$took, 20)
})

test_that("a node that cannot be replaced stops set-up by default, and is dropped on request", {
  cl <- localCluster(3L)
  pids <- attr(cl, "pids")
  withr::defer(killAll(pids))
  tools::pskill(pids[3], tools::SIGKILL)
  Sys.sleep(0.5)
  start <- function(host) stop("no route to ", host)
  expect_error(suppressMessages(.replaceDeadNodes(cl, start, seconds = 5, tries = 2L, action = "stop")),
               "does not answer and 2 replacement")
  out <- suppressMessages(.replaceDeadNodes(cl, start, seconds = 5, tries = 2L, action = "drop"))
  expect_length(out$cluster, 2L)
  expect_identical(out$dropped, 3L)
  expect_equal(unlist(parallel::clusterCall(out$cluster, function() 1L)), c(1L, 1L))
  parallel::stopCluster(out$cluster)
})

test_that("a worker stopped mid-evaluation: the call returns on a rebuilt cluster within the timeout", {
  flag <- withr::local_tempfile()
  withr::local_options(clusters.workerTimeout = 3)
  allPids <- integer(0)
  withr::defer(killAll(allPids))
  make <- function() {
    cl <- localCluster(3L)
    allPids <<- c(allPids, attr(cl, "pids"))
    .setWorkerTimeout(cl)
    cl
  }
  restarts <- 0L
  cl <- make()
  attr(cl, "restartCluster") <- function(old) {
    restarts <<- restarts + 1L
    .stopNodes(old)
    new <- make()
    attr(new, "restartCluster") <- attr(old, "restartCluster")
    new
  }
  run <- function(cl) {
    parallel::parSapply(cl, 1:3, function(i, flag) {
      if (i == 2L && !file.exists(flag)) {
        file.create(flag)
        tools::pskill(Sys.getpid(), tools::SIGSTOP)   # gone, socket still open
      }
      i * 10L
    }, flag = flag)
  }
  took <- system.time(res <- suppressMessages(.runWithRebuild(cl, run, retries = 2L)))[["elapsed"]]
  expect_equal(res$value, c(10L, 20L, 30L))
  expect_identical(restarts, 1L)
  expect_false(identical(res$cluster, cl))
  expect_lt(took, 60)
  parallel::stopCluster(res$cluster)
})

## A rebuild whose hosts are named by `cores`; workers are local, labelled with the host they stand for.
## `startNodes(hosts, autoStop)` is what plan_psock_min() gives .restartClusterFn(); the real one needs ssh.
fakeRebuild <- function(cores, reachable, failStart = character(0), env = parent.frame()) {
  ## what is sent to the workers needs ssh and rsync for a host that is not this one
  testthat::local_mocked_bindings(.shipToWorkers = function(cl, cores, ...) invisible(cl), .package = "clusters",
                                  .env = env)
  started <- character(0)
  startNodes <- function(hosts, autoStop = TRUE) {
    if (any(hosts %in% failStart)) stop("Failed to launch and connect to R worker on remote machine '",
                                         hosts[hosts %in% failStart][1], "'")
    started <<- c(started, hosts)
    cl <- localTestCluster(length(hosts), env = env, stop = .stopCluster)
    for (i in seq_along(cl)) cl[[i]]$host <- hosts[i]
    cl
  }
  restart <- .restartClusterFn(startNodes, cores, character(0), character(0), environment(), digest = NULL,
                               token = NULL, probe = function(host) host %in% reachable)
  list(restart = restart, started = function() started)
}

test_that("a rebuild leaves out a host that does not answer, and says how many workers that cost", {
  cores <- c("hostA", "hostA", "hostB", "hostB", "hostB")
  fake <- fakeRebuild(cores, reachable = "hostA")
  old <- localTestCluster(5L, stop = .stopCluster)
  expect_message(new <- suppressWarnings(fake$restart(old)), "hostB.*3 worker")
  expect_length(new, 2L)
  expect_identical(.nodeHosts(new), "hostA x2")
  expect_false("hostB" %in% fake$started())
  expect_equal(unlist(parallel::clusterCall(new, function() 1L)), c(1L, 1L))
  ## the next rebuild starts only what is left
  expect_length(suppressMessages(attr(new, "restartCluster")(new)), 2L)
  expect_false("hostB" %in% fake$started())
})

test_that("a rebuild drops the workers of a host that answers the probe but cannot start a worker", {
  fake <- fakeRebuild(c("hostA", "hostB", "hostB"), reachable = c("hostA", "hostB"), failStart = "hostB")
  new <- suppressMessages(fake$restart(localTestCluster(3L, stop = .stopCluster)))
  expect_identical(.nodeHosts(new), "hostA x1")
})

test_that("a rebuild is an error only when no host is left", {
  fake <- fakeRebuild(c("hostA", "hostB"), reachable = character(0))
  expect_error(suppressMessages(fake$restart(localTestCluster(2L, stop = .stopCluster))), "no worker")
  fake <- fakeRebuild(c("hostA", "hostB"), reachable = c("hostA", "hostB"), failStart = c("hostA", "hostB"))
  expect_error(suppressMessages(fake$restart(localTestCluster(2L, stop = .stopCluster))), "no worker")
})

test_that("localhost is never probed: it stays when the probe fails every host", {
  fake <- fakeRebuild(c("localhost", "localhost", "hostB"), reachable = character(0))
  new <- suppressMessages(fake$restart(localTestCluster(3L, stop = .stopCluster)))
  expect_identical(.nodeHosts(new), "localhost x2")
})

test_that("with no way to rebuild, a stopped worker is an error that names the workers, not a hang", {
  flag <- withr::local_tempfile()
  withr::local_options(clusters.workerTimeout = 2)
  cl <- localCluster(2L)
  withr::defer(killAll(attr(cl, "pids")))
  .setWorkerTimeout(cl)
  run <- function(cl) parallel::parSapply(cl, 1:2, function(i) {
    if (i == 1L) tools::pskill(Sys.getpid(), tools::SIGSTOP)
    i
  })
  took <- system.time(err <- tryCatch(.runWithRebuild(cl, run), error = function(e) e))[["elapsed"]]
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "localhost x2")
  expect_match(conditionMessage(err), "cannot be rebuilt")
  expect_lt(took, 30)
})

test_that("an error raised by the objective function is not treated as a lost worker", {
  cl <- localCluster(2L)
  withr::defer(killAll(attr(cl, "pids")))
  restarts <- 0L
  attr(cl, "restartCluster") <- function(old) { restarts <<- restarts + 1L; old }
  expect_error(.runWithRebuild(cl, function(cl) parallel::parSapply(cl, 1:2, function(i) stop("boom"))),
               "boom")
  expect_identical(restarts, 0L)
  parallel::stopCluster(cl)
})

test_that(".setWorkerTimeout() replaces the 30-day socket timeout", {
  cl <- localCluster(1L)
  withr::defer(killAll(attr(cl, "pids")))
  expect_gt(socketTimeout(cl[[1]]$con, 30), 30)          # returns the old value; this one is set to 30
  .setWorkerTimeout(cl, 123)
  expect_equal(socketTimeout(cl[[1]]$con, 123), 123)
  withr::local_options(clusters.workerTimeout = -1)
  expect_error(.workerTimeout(), "positive")
  parallel::stopCluster(cl)
})

test_that("a stopped autoStop cluster, when garbage collected, leaves alone the connections that reuse its numbers", {
  ## CI, 2026-10-01: parallelly's autoStop finalizer stopped an already-stopped cluster again and closed a
  ## later test's live connection, which had been given the same number ("invalid connection")
  for (stopper in list(.stopCluster, .stopNodes)) {
    old <- parallelly::makeClusterPSOCK(1L, autoStop = TRUE)
    stopper(old)
    cl <- localCluster(1L)
    withr::defer(killAll(attr(cl, "pids")))
    skip_if_not(identical(as.integer(cl[[1]]$con), as.integer(old[[1]]$con)), "R did not reuse the number")
    rm(old)
    invisible(gc())
    expect_equal(unlist(parallel::clusterCall(cl, function() 1L)), 1L)
    parallel::stopCluster(cl)
  }
})

test_that("stopping a cluster a second time leaves alone the connections that reuse its numbers", {
  old <- localCluster(1L)
  withr::defer(killAll(attr(old, "pids")))
  .stopCluster(old)
  cl <- localCluster(1L)
  withr::defer(killAll(attr(cl, "pids")))
  skip_if_not(identical(as.integer(cl[[1]]$con), as.integer(old[[1]]$con)), "R did not reuse the number")
  .stopCluster(old)
  .stopNodes(old)
  expect_equal(unlist(parallel::clusterCall(cl, function() 1L)), 1L)
  .stopCluster(cl)
})

test_that("autoStop stops a cluster's replacement nodes, not the closed ones they replaced", {
  cl <- parallelly::makeClusterPSOCK(2L, autoStop = TRUE)
  pids <- unlist(parallel::clusterCall(cl, Sys.getpid))
  withr::defer(killAll(pids))
  tools::pskill(pids[2], tools::SIGKILL)
  Sys.sleep(0.5)
  start <- function(host) {
    new <- localCluster(1L)
    pids <<- c(pids, attr(new, "pids"))
    new
  }
  out <- suppressMessages(.replaceDeadNodes(cl, start, seconds = 5, tries = 2L, action = "stop"))
  expect_identical(attr(out$cluster, "gcMe")$cluster, out$cluster)
  .stopCluster(out$cluster)
})

test_that("DEoptimIterative() finishes with the result of a run with no dead worker when one worker stops", {
  skip_if_not_installed("DEoptim")
  skip_if(isTRUE(parallel::detectCores() < 4L), "needs 4 cores")
  ## the workers run clusters' recording wrapper, so they must load a clusters that has it
  probe <- localCluster(1L)
  ok <- unlist(parallel::clusterCall(probe, function(libs) {
    .libPaths(libs)
    requireNamespace("clusters", quietly = TRUE) &&
      exists(".workerTimeout", envir = asNamespace("clusters"), inherits = FALSE)
  }, .libPaths()))
  killAll(attr(probe, "pids")); try(parallel::stopCluster(probe), silent = TRUE)
  skip_if_not(all(ok), "the installed clusters on the workers predates this change")

  lower <- c(a = 0, b = 0, c = 0); upper <- c(a = 1, b = 1, c = 1)
  withr::local_options(clusters.workerTimeout = 4, reproducible.useCache = FALSE,
                       reproducible.cachePath = withr::local_tempdir())
  flag <- withr::local_tempfile()
  allPids <- integer(0)
  withr::defer(killAll(allPids))
  make <- function() {
    cl <- localCluster(4L)
    allPids <<- c(allPids, attr(cl, "pids"))
    parallel::clusterCall(cl, function(libs) .libPaths(libs), .libPaths())
    .setWorkerTimeout(cl)
    attr(cl, "restartCluster") <- function(old) { .stopNodes(old); make() }
    cl
  }
  fn <- function(par, flag, stopOnce) {
    if (stopOnce && !file.exists(flag) && par[1] > 0.5) {
      file.create(flag)
      tools::pskill(Sys.getpid(), tools::SIGSTOP)
    }
    sum((par - 0.3)^2)
  }
  run <- function(stopOnce) {
    if (file.exists(flag)) file.remove(flag)
    cl <- make()
    on.exit(try(parallel::stopCluster(cl), silent = TRUE))
    set.seed(42)
    suppressWarnings(suppressMessages(clusters::DEoptimIterative(
      fn, lower = lower, upper = upper,
      control = list(NP = 4L, strategy = 2L, itermax = 3, trace = FALSE, cluster = cl),
      flag = flag, stopOnce = stopOnce,
      figurePath = FALSE, progressFile = FALSE, .plots = NULL, runName = "dead", .verbose = -1)))
  }
  clean <- run(FALSE)
  took <- system.time(withDeath <- run(TRUE))[["elapsed"]]
  expect_true(file.exists(flag))                          # a worker did stop
  expect_lt(took, 120)
  expect_equal(withDeath[[3]]$optim$bestmem, clean[[3]]$optim$bestmem)
  expect_equal(withDeath[[3]]$member$pop, clean[[3]]$member$pop)
})
