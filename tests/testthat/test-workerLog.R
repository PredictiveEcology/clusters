## FireSense 2026-10-07: run outputs, and so the worker log folder, were on /mnt/fast, a disk only the
## master has. Every worker on the other hosts opened its log, could not, and exited before connecting
## back; each spread fit failed after five tries. A host that cannot write the log folder now logs to
## its own user cache folder.

## A log path whose folder cannot be created: under a folder nobody may write to
unwritableLogPath <- function(env = parent.frame()) {
  locked <- withr::local_tempdir(.local_envir = env)
  Sys.chmod(locked, "0555")
  withr::defer(Sys.chmod(locked, "0755"), envir = env)
  file.path(locked, "log", "fit_pid1.log")
}

test_that("a worker uses the expected log path when it can write there", {
  logPath <- file.path(withr::local_tempdir(), "log", "fit_pid1.log")
  expect_identical(clusters:::.workerLogPath(logPath), logPath)
  expect_true(dir.exists(dirname(logPath)))
})

test_that("a worker that cannot write the log folder logs to its own cache folder", {
  skip_on_os("windows")
  skip_if(identical(unname(Sys.info()[["effective_user"]]), "root"), "root writes anywhere")
  withr::local_envvar(R_USER_CACHE_DIR = withr::local_tempdir())
  logPath <- unwritableLogPath()
  own <- clusters:::.workerLogPath(logPath)
  expect_false(identical(own, logPath))
  expect_true(dir.exists(dirname(own)))
  expect_identical(basename(own), paste0(Sys.info()[["nodename"]], "_fit_pid1.log"))
  expect_identical(unname(file.access(dirname(own), 2L)), 0L)
})

test_that("each host's log is asked of that host, named by host, and a moved log is reported", {
  skip_on_cran()
  skip_on_os("windows")
  skip_if(identical(unname(Sys.info()[["effective_user"]]), "root"), "root writes anywhere")
  withr::local_envvar(R_USER_CACHE_DIR = withr::local_tempdir())
  logPath <- unwritableLogPath()
  cl <- localTestCluster(2)    # stands for two hosts
  expect_message(logs <- clusters:::.hostLogPaths(cl, c("hostA", "hostB"), logPath),
                 "cannot be written on hostA, hostB")
  expect_named(logs, c("hostA", "hostB"))
  expect_true(all(startsWith(logs, Sys.getenv("R_USER_CACHE_DIR"))))
  expect_null(clusters:::.hostLogPaths(cl, c("hostA", "hostB"), NULL))
})

test_that("a worker starts when the log folder cannot be written on its host", {
  skip_on_cran()
  skip_on_os("windows")
  skip_if(identical(unname(Sys.info()[["effective_user"]]), "root"), "root writes anywhere")
  withr::local_envvar(R_USER_CACHE_DIR = withr::local_tempdir())
  logPath <- unwritableLogPath()
  probe <- localTestCluster(1)
  logs <- suppressMessages(clusters:::.hostLogPaths(probe, "localhost", logPath))
  cl <- clusters::makeClusterPSOCK("localhost", outfile = logs, rscript_libs = .libPaths(),
                                   revtunnel = FALSE, renice = NA, tries = 1L, connectTimeout = 60)
  withr::defer(parallel::stopCluster(cl))
  expect_identical(parallel::clusterEvalQ(cl, 1L + 1L)[[1]], 2L)
  expect_true(file.exists(logs[["localhost"]]))
})

test_that("one outfile serves every host; a per-host outfile is looked up by host", {
  expect_null(clusters:::.hostOutfile(NULL, "a"))
  expect_identical(clusters:::.hostOutfile("x.log", "a"), "x.log")
  expect_identical(clusters:::.hostOutfile(c(a = "a.log", b = "b.log"), "b"), "b.log")
})
