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
            ".workerTimeout", ".sshTunnelOpts"))
  assign(f, getFromNamespace(f, "clusters"))

localCluster <- function(n) {
  cl <- parallel::makeCluster(n)
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
      figurePath = FALSE, .plots = NULL, runName = "dead", .verbose = -1)))
  }
  clean <- run(FALSE)
  took <- system.time(withDeath <- run(TRUE))[["elapsed"]]
  expect_true(file.exists(flag))                          # a worker did stop
  expect_lt(took, 120)
  expect_equal(withDeath[[3]]$optim$bestmem, clean[[3]]$optim$bestmem)
  expect_equal(withDeath[[3]]$member$pop, clean[[3]]$member$pop)
})
