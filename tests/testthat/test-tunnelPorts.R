## clusters' makeClusterPSOCK() picks the port block for the master and the workers' reverse tunnels.
##
## FireSense settings study, 2026-09-15: a 110-worker build hung at "Starting main parallel cluster ...".
## Its reverse tunnels used consecutive ports from the master's, 33962 up. On camas a worker's own
## connection had been given 33982 as its ephemeral (outgoing) port, so the next worker's `ssh -R 33982`
## could not bind and that worker could not reach the master ("cannot open the connection"). The block
## was drawn from 20000:40000, and every host's ephemeral range is 32768-60999: about a third of all
## draws put tunnel ports where any connection on a host can take them first.

## What parallelly's makeClusterPSOCK() returns for one worker that is not a socket connection
fakeCluster <- function() structure(list(list(host = "localhost")), class = c("SOCKcluster", "cluster"))

test_that("the port block, plus one tunnel port per worker, ends below the ephemeral port range", {
  seen <- new.env()
  seen$ports <- list()
  testthat::local_mocked_bindings(
    makeClusterPSOCK = function(workers, port, ...) {
      seen$ports[[length(seen$ports) + 1L]] <- port
      fakeCluster()
    },
    .package = "parallelly")
  workers <- rep("localhost", 110)
  withr::local_seed(20260915)
  firstPorts <- numeric(0)
  for (i in 1:30) {
    seen$ports <- list()
    clusters::makeClusterPSOCK(workers)
    ## one start per worker, each on its own port: tunnels to one host must not share a port
    expect_length(seen$ports, length(workers))
    ports <- unlist(seen$ports)
    expect_false(anyDuplicated(ports) > 0)
    expect_lt(max(ports), 32768)                             # Linux's default range starts here
    expect_lt(max(ports), clusters:::.ephemeralPortStart())  # and this machine's
    expect_gte(min(ports), 1024)
    firstPorts <- c(firstPorts, min(ports))
  }
  ## still a spread of blocks, so concurrent masters rarely share one
  expect_gt(length(unique(firstPorts)), 20L)
})

test_that("a worker count given as a number is counted as that many tunnel ports", {
  seen <- new.env()
  testthat::local_mocked_bindings(
    makeClusterPSOCK = function(workers, port, ...) { seen$ports <- c(seen$ports, port); fakeCluster() },
    .package = "parallelly")
  withr::local_seed(1)
  for (i in 1:5) {
    seen$ports <- NULL
    clusters::makeClusterPSOCK(2000L)
    expect_length(seen$ports, 2000L)
    expect_lt(max(seen$ports), 32768)
  }
})

test_that("each worker is started with its own element of a per-worker `user`", {
  seen <- new.env()
  seen$users <- list()
  testthat::local_mocked_bindings(
    makeClusterPSOCK = function(workers, user = NULL, ...) {
      seen$users[[length(seen$users) + 1L]] <- user
      fakeCluster()
    },
    .package = "parallelly")
  clusters::makeClusterPSOCK(c("a", "b", "c"), user = c("u1", "u2", "u3"))
  expect_identical(unlist(seen$users), c("u1", "u2", "u3"))
  seen$users <- list()
  clusters::makeClusterPSOCK(c("a", "b"), user = "u")
  expect_identical(unlist(seen$users), c("u", "u"))
  seen$users <- list()
  clusters::makeClusterPSOCK(c("a", "b"))
  expect_length(seen$users, 0L)  # NULL stays NULL
})

test_that("the ephemeral range start is read from the kernel setting, with Linux's default otherwise", {
  f <- withr::local_tempfile()
  writeLines("40000\t60999", f)
  expect_identical(clusters:::.ephemeralPortStart(f), 40000L)
  expect_identical(clusters:::.ephemeralPortStart(file.path(withr::local_tempdir(), "missing")), 32768L)
  writeLines("not a number", f)
  expect_identical(clusters:::.ephemeralPortStart(f), 32768L)
})

test_that("a worker that never connects back fails the start within connectTimeout, not timeout", {
  ## FireSense 02e, 2026-10-02/03: ssh exited on "remote port forwarding failed for listen port N"
  ## (ExitOnForwardFailure=yes), so the worker never started, and five masters waited 5-17 hours in
  ## parallelly's socketConnection(server = TRUE, timeout = 30 days). `false` stands in for that
  ## ssh: it exits at once and nothing connects back.
  t0 <- Sys.time()
  expect_message(
    ## suppressWarnings: testthat's own warning handler is slow enough to trip parallelly's time limit
    expect_error(suppressWarnings(
      clusters::makeClusterPSOCK("localhost", rscript = "false", homogeneous = FALSE,
                                 connectTimeout = 2, timeout = 120, tries = 2L, delay = 0)),
      class = "PSOCKConnectionError"),
    "another port block")
  expect_lt(as.numeric(difftime(Sys.time(), t0, units = "secs")), 60)
})

test_that("with several workers, one that never connects back fails the start within connectTimeout", {
  skip_on_cran()
  skip_on_os("windows")  # the stand-in Rscript is a shell script
  ## FireSense, 2026-10-04: with 40 workers and connectTimeout 120, a worker whose ssh tunnel failed
  ## blocked the master for 4800 s, because the start gave every worker connectTimeout times the
  ## number of workers. Here the first worker starts, the second and third never connect back.
  tmp <- withr::local_tempdir()
  marker <- file.path(tmp, "first")
  fake <- file.path(tmp, "Rscript")
  writeLines(c("#!/bin/sh",
               sprintf('case "$*" in *workRSOCK*) [ -f "%s" ] && exit 1; touch "%s" ;; esac', marker, marker),
               sprintf('exec "%s" "$@"', file.path(R.home("bin"), "Rscript"))), fake)
  Sys.chmod(fake, "0755")
  t0 <- Sys.time()
  movedAfter <- NA_real_
  res <- withCallingHandlers(
    tryCatch(suppressWarnings(
      clusters::makeClusterPSOCK(rep("localhost", 3), rscript = fake, homogeneous = FALSE,
                                 connectTimeout = 6, tries = 2L, delay = 0, renice = FALSE)),
      error = identity),
    message = function(m) {
      if (grepl("another port block", conditionMessage(m)) && is.na(movedAfter))
        movedAfter <<- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    })
  expect_s3_class(res, "PSOCKConnectionError")
  ## the move to another port block comes after one connectTimeout (and parallelly's few seconds of
  ## clean-up for a failed worker), not after connectTimeout times 3 workers, which took 24 s
  expect_lt(movedAfter, 17)
  ## and no time limit of the failed start is left to fire in the caller's code
  later <- tryCatch({ Sys.sleep(5); "quiet" }, error = function(e) conditionMessage(e))
  expect_identical(later, "quiet")
})

test_that("starting a cluster does not load clusters on the workers", {
  skip_on_cran()
  ## covr puts covr:::count() calls into every clusters function, including the one sent to the
  ## workers; running them there loads covr, which loads clusters to record the counts
  skip_on_covr()
  ## FireSense, 2026-10-04: the call that sets the workers' timeout sent a function whose environment
  ## was the clusters namespace, so each fresh worker loaded clusters and its dependencies to read it:
  ## 1.5 s per worker, paid one worker after another
  cl <- clusters::makeClusterPSOCK("localhost", homogeneous = FALSE,
                                   rscript = file.path(R.home("bin"), "Rscript"), renice = FALSE)
  withr::defer(parallel::stopCluster(cl))
  expect_false(parallel::clusterEvalQ(cl, "clusters" %in% loadedNamespaces())[[1]])
})

test_that("a started cluster has the long read timeout, not the connect timeout", {
  skip_on_cran()
  ## homogeneous = FALSE starts workers one at a time, as for remote hosts; it also takes `Rscript`
  ## from PATH, which on CI (R-devel, macOS) is not the R under check, so the worker never started:
  ## give it this R's Rscript. A generous connectTimeout: this test is about the read timeout after
  ## the start.
  cl <- clusters::makeClusterPSOCK("localhost", homogeneous = FALSE, connectTimeout = 300,
                                   rscript = file.path(R.home("bin"), "Rscript"), timeout = 1234,
                                   renice = FALSE)  # macOS nice has no --adjustment
  withr::defer(parallel::stopCluster(cl))
  expect_identical(parallel::clusterEvalQ(cl, 1L)[[1]], 1L)
  expect_equal(socketTimeout(cl[[1]]$con), 1234)
  ## and at the worker's end, where parallelly had put connectTimeout
  workerSide <- parallel::clusterEvalQ(cl, {
    socks <- Filter(function(con) inherits(con, "sockconn"), lapply(getAllConnections(), getConnection))
    vapply(socks, socketTimeout, numeric(1))
  })[[1]]
  expect_equal(unique(workerSide), 1234)
})

test_that("a worker idle for longer than connectTimeout still answers", {
  skip_on_cran()
  skip_on_ci()  # a 5 s connectTimeout is too short for CI's localhost starts; the test above covers CI
  ## FireSense, 2026-10-03: parallelly gave the workers `connectTimeout` as their read timeout, so a
  ## worker quit after that long without a call, and the next call failed with "error reading from
  ## connection"
  cl <- clusters::makeClusterPSOCK("localhost", homogeneous = FALSE, connectTimeout = 5,
                                   rscript = file.path(R.home("bin"), "Rscript"), renice = FALSE)
  withr::defer(parallel::stopCluster(cl))
  expect_identical(parallel::clusterEvalQ(cl, 1L)[[1]], 1L)
  Sys.sleep(8)
  expect_identical(parallel::clusterEvalQ(cl, 2L)[[1]], 2L)
})

test_that("the first worker does not quit while a slow one is still starting", {
  skip_on_cran()
  skip_on_ci()  # timing: needs the first worker to connect within a few seconds
  skip_on_os("windows")  # the stand-in Rscript is a shell script
  ## FireSense, 2026-10-03: 12 clusters starting at once took 165 s to start with connectTimeout 120;
  ## the workers that connected first had quit by then. Here every worker after the first starts 3 s
  ## late, within connectTimeout, so the first one waits 9 s in all, longer than connectTimeout.
  tmp <- withr::local_tempdir()
  marker <- file.path(tmp, "first")
  fake <- file.path(tmp, "Rscript")
  writeLines(c("#!/bin/sh",
               sprintf('case "$*" in *workRSOCK*) [ -f "%s" ] && sleep 3; touch "%s" ;; esac', marker, marker),
               sprintf('exec "%s" "$@"', file.path(R.home("bin"), "Rscript"))), fake)
  Sys.chmod(fake, "0755")
  cl <- clusters::makeClusterPSOCK(rep("localhost", 4), homogeneous = FALSE, connectTimeout = 6,
                                   rscript = fake, renice = FALSE)
  withr::defer(parallel::stopCluster(cl))
  expect_identical(unlist(parallel::clusterEvalQ(cl, 1L)), rep(1L, 4))
})

test_that("a worker that fails to connect once is started on the retry", {
  skip_on_cran()
  skip_on_os("windows")  # the stand-in Rscript is a shell script
  ## A stand-in Rscript that makes the first worker launch exit without connecting (parallelly's
  ## PID probe, which runs it first, still works); every later call is the real Rscript. A retry
  ## must then succeed: it gets a fresh wait, not what is left of the first attempt's.
  tmp <- withr::local_tempdir()
  marker <- file.path(tmp, "failedOnce")
  fake <- file.path(tmp, "Rscript")
  writeLines(c("#!/bin/sh",
               sprintf('case "$*" in *workRSOCK*) [ -f "%s" ] || { touch "%s"; exit 1; } ;; esac', marker, marker),
               sprintf('exec "%s" "$@"', file.path(R.home("bin"), "Rscript"))), fake)
  Sys.chmod(fake, "0755")
  ## suppressWarnings: the failed listen warns just as its time limit expires, and testthat's own
  ## warning handler would then be the code that hits the limit
  expect_message(suppressWarnings(
    cl <- clusters::makeClusterPSOCK("localhost", homogeneous = FALSE, rscript = fake,
                                     connectTimeout = 15, tries = 2L, delay = 1, timeout = 1234,
                                     renice = FALSE)),  # macOS nice has no --adjustment
    "another port block")
  withr::defer(parallel::stopCluster(cl))
  expect_true(file.exists(marker))
  expect_identical(parallel::clusterEvalQ(cl, 1L)[[1]], 1L)
  expect_equal(socketTimeout(cl[[1]]$con), 1234)
})

test_that("a slow handler of the socket's timeout warning does not turn a failed start into a time-limit error", {
  skip_on_cran()
  skip_on_os("windows")
  ## FireSense, 2026-10-04: socketConnection(server = TRUE) warns when its wait times out. Cache and
  ## SpaDES re-signal that warning from calling handlers, which take seconds, and they run while
  ## parallelly's setTimeLimit(elapsed = connectTimeout) is still set. The limit had expired with the
  ## socket's own timeout, so the "reached elapsed time limit" error was raised inside the handler, out
  ## of reach of clusters' tryCatch, and ended the caller. The outer handler here stands in for them.
  res <- tryCatch(
    withCallingHandlers(
      clusters::makeClusterPSOCK("localhost", rscript = "false", homogeneous = FALSE,
                                 connectTimeout = 5, tries = 2L, delay = 0),
      warning = function(w) Sys.sleep(3)),
    error = identity)
  expect_s3_class(res, "PSOCKConnectionError")
  expect_false(grepl("elapsed time limit", conditionMessage(res)))
  later <- tryCatch({ Sys.sleep(4); "quiet" }, error = function(e) conditionMessage(e))
  expect_identical(later, "quiet")
})
