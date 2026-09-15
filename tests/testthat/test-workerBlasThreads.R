## Cap OpenBLAS threads in cluster workers.
##
## R linked to multithreaded OpenBLAS starts one thread per logical CPU (up to 64) when R
## starts. An objective function's small matrix products then wake all of them: on the
## FireSense fleet (2026-09-15) each DEoptim worker held 49 threads and used ~6 CPUs, while
## the allocator counts one CPU per worker, and a 50,000 x 12 product ran 2-3x slower with
## the pool than with one thread. The cap has to prefix the worker command: parallelly's
## `rscript_envs` sets variables with Sys.setenv() after R has started, which is too late
## (verified: 64 threads with rscript_envs, 1 with an `env` prefix).

test_that("the worker command is prefixed with OPENBLAS_NUM_THREADS=1 by default", {
  withr::local_options(clusters.workerBlasThreads = NULL)
  expect_identical(clusters:::.workerRscript("Rscript", blasThreads = 1L),
                   c("env", "OPENBLAS_NUM_THREADS=1", "Rscript"))
  ## the default comes from the option, which defaults to 1
  expect_identical(clusters:::.workerRscript("Rscript"),
                   c("env", "OPENBLAS_NUM_THREADS=1", "Rscript"))
})

test_that("the option changes the cap, and NA leaves the command alone", {
  withr::local_options(clusters.workerBlasThreads = 4L)
  expect_identical(clusters:::.workerRscript("Rscript"),
                   c("env", "OPENBLAS_NUM_THREADS=4", "Rscript"))
  withr::local_options(clusters.workerBlasThreads = NA)
  expect_identical(clusters:::.workerRscript("Rscript"), "Rscript")
})

test_that("an existing prefix is kept", {
  expect_identical(clusters:::.workerRscript(c("env", "LD_LIBRARY_PATH=/x", "Rscript"), blasThreads = 1L),
                   c("env", "OPENBLAS_NUM_THREADS=1", "env", "LD_LIBRARY_PATH=/x", "Rscript"))
})

test_that("plan_psock_min launches workers through the capped command", {
  src <- paste(deparse(clusters:::plan_psock_min), collapse = "\n")
  expect_match(src, "rscript <- .workerRscript(rscript)", fixed = TRUE)
})

test_that("a prefixed worker command stays runnable under nice (clusters' makeClusterPSOCK renices)", {
  ## 2026-09-15: the first real cluster built with the prefix failed on every host with
  ##   nice: 'R_DEFAULT_PACKAGES=datasets,...': No such file or directory
  ## parallelly sees `env`, not Rscript, as the program, so it passes the default packages as a
  ## R_DEFAULT_PACKAGES= assignment in front of the command -- and renice puts `nice` in front of that.
  ## a dry run prints the worker command (as a message) instead of starting the worker
  msgs <- character()
  out <- capture.output(withCallingHandlers(
    clusters::makeClusterPSOCK("localhost", port = 11111L, dryrun = TRUE,
                               rscript = clusters:::.workerRscript("Rscript", blasThreads = 1L)),
    message = function(m) { msgs <<- c(msgs, conditionMessage(m)); invokeRestart("muffleMessage") }))
  lines <- unlist(strsplit(c(out, msgs), "\n", fixed = TRUE))
  cmd <- grep("OPENBLAS_NUM_THREADS", lines, value = TRUE)[1]
  expect_length(cmd, 1L)
  expect_match(cmd, "nice --adjustment=20 ", fixed = TRUE)   # the wrapper's renice is still applied
  expect_false(grepl("nice --adjustment=[0-9]+ R_DEFAULT_PACKAGES=", cmd))
  ## the default packages travel inside env's assignments instead
  expect_match(cmd, "env'? OPENBLAS_NUM_THREADS=1 R_DEFAULT_PACKAGES=datasets,utils,grDevices,graphics,stats,methods")
})

test_that("a worker launched through clusters' makeClusterPSOCK starts, capped and with the default packages", {
  skip_on_cran()
  skip_on_os(c("windows", "mac"))
  cl <- clusters::makeClusterPSOCK(
    1L, rscript = clusters:::.workerRscript(file.path(R.home("bin"), "Rscript"), blasThreads = 1L),
    autoStop = TRUE)
  on.exit(parallel::stopCluster(cl), add = TRUE)
  got <- parallel::clusterEvalQ(cl, list(env = Sys.getenv("OPENBLAS_NUM_THREADS"), search = search()))[[1]]
  expect_identical(got$env, "1")
  expect_true(all(paste0("package:", c("stats", "utils", "methods")) %in% got$search))
})

test_that("a worker started this way has one OpenBLAS thread", {
  skip_on_cran()
  skip_on_os(c("windows", "mac"))   # `env` and /proc/self/task are what this checks
  cl <- parallelly::makeClusterPSOCK(
    1L, rscript = clusters:::.workerRscript(file.path(R.home("bin"), "Rscript"), blasThreads = 1L),
    autoStop = TRUE)
  on.exit(parallel::stopCluster(cl), add = TRUE)
  got <- parallel::clusterEvalQ(cl, list(env = Sys.getenv("OPENBLAS_NUM_THREADS"),
                                         threads = length(list.files("/proc/self/task"))))[[1]]
  expect_identical(got$env, "1")
  ## Only meaningful where R uses a threaded OpenBLAS: one thread instead of one per CPU.
  masterThreads <- length(list.files("/proc/self/task"))
  if (masterThreads > 2L) expect_lte(got$threads, 2L)
})
