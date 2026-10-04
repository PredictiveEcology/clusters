## clusters' makeClusterPSOCK() picks the port block for the master and the workers' reverse tunnels.
##
## FireSense settings study, 2026-09-15: a 110-worker build hung at "Starting main parallel cluster ...".
## Its reverse tunnels used consecutive ports from the master's, 33962 up. On camas a worker's own
## connection had been given 33982 as its ephemeral (outgoing) port, so the next worker's `ssh -R 33982`
## could not bind and that worker could not reach the master ("cannot open the connection"). The block
## was drawn from 20000:40000, and every host's ephemeral range is 32768-60999: about a third of all
## draws put tunnel ports where any connection on a host can take them first.

test_that("the port block, plus one tunnel port per worker, ends below the ephemeral port range", {
  seen <- new.env()
  seen$ports <- list()
  testthat::local_mocked_bindings(
    makeClusterPSOCK = function(workers, port, ...) {
      seen$ports[[length(seen$ports) + 1L]] <- port
      invisible(NULL)
    },
    .package = "parallelly")
  workers <- rep("localhost", 110)
  withr::local_seed(20260915)
  for (i in 1:300) clusters::makeClusterPSOCK(workers)

  expect_length(seen$ports, 300L)
  lastTunnelPort <- vapply(seen$ports, function(p) max(p) + length(workers) - 1, numeric(1))
  expect_lt(max(lastTunnelPort), 32768)                             # Linux's default range starts here
  expect_lt(max(lastTunnelPort), clusters:::.ephemeralPortStart())  # and this machine's
  expect_gte(min(vapply(seen$ports, min, numeric(1))), 1024)
  ## still a spread of blocks, so concurrent masters rarely share one
  expect_gt(length(unique(vapply(seen$ports, min, numeric(1)))), 100L)
})

test_that("a worker count given as a number is counted as that many tunnel ports", {
  seen <- new.env()
  testthat::local_mocked_bindings(
    makeClusterPSOCK = function(workers, port, ...) { seen$port <- port; invisible(NULL) },
    .package = "parallelly")
  withr::local_seed(1)
  for (i in 1:200) {
    clusters::makeClusterPSOCK(2000L)
    expect_lt(max(seen$port) + 2000 - 1, 32768)
  }
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
