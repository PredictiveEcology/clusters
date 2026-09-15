## clusterSetup() moves the caller's objects (`objsNeeded`, found in `envir`) to the workers.
## FireSense settings study, 2026-09-15: run from a plain script, where reproducible is not attached,
## the move failed at `Filenames()`, which clusters did not import, and the fallback then looked for
## the objects in clusterSetup()'s own frame instead of `envir`: "object 'x1' not found".
## SpaDES attaches reproducible, which hid both.

## Builds a 2-worker local cluster and returns what each worker has for `x1`. The cluster lives as
## long as this function's frame (plan_psock_min stops it when a frame above clusterSetup() exits),
## so messages are captured by the caller, outside this function.
objectsOnWorkers <- function() {
  x1 <- c(a = 1, b = 2)
  control <- clusters::clusterSetup(
    messagePrefix = "objects", itermax = 1, trace = FALSE, cores = "localhost",
    logPath = withr::local_tempdir(), libPath = .libPaths()[1],
    objsNeeded = list("x1"), pkgsNeeded = "stats", nCoresNeeded = 2L, envir = environment())
  on.exit(try(parallel::stopCluster(control$cluster), silent = TRUE), add = TRUE)
  parallel::clusterEvalQ(control$cluster, get0("x1", envir = .GlobalEnv))
}

localClusterOptions <- function(env = parent.frame()) {
  withr::local_options(clusters.reservationsPath = withr::local_tempfile(fileext = ".rds", .local_envir = env),
                       clusters.waitForCores = 0, clusters.minWorkersFraction = 1, .local_envir = env)
  ## clusterSetup() also puts the objects in the master's global environment for local workers
  withr::defer(if (exists("x1", envir = .GlobalEnv, inherits = FALSE)) rm("x1", envir = .GlobalEnv), envir = env)
}

collectMessages <- function(expr) {
  msgs <- character()
  value <- withCallingHandlers(expr, message = function(m) {
    msgs <<- c(msgs, conditionMessage(m))
    invokeRestart("muffleMessage")
  })
  list(value = value, messages = msgs)
}

test_that("clusterSetup() moves objects from envir to the workers when reproducible is not attached", {
  skip_on_cran()
  skip_on_os(c("windows", "mac"))
  skip_if("package:reproducible" %in% search(), "reproducible is attached")
  localClusterOptions()

  out <- collectMessages(objectsOnWorkers())
  expect_false(any(grepl("trying clusterExport", out$messages)))
  expect_length(out$value, 2L)
  for (w in out$value) expect_identical(w, c(a = 1, b = 2))
})

test_that("when the file transfer fails, the fallback exports the objects from envir", {
  skip_on_cran()
  skip_on_os(c("windows", "mac"))
  localClusterOptions()
  testthat::local_mocked_bindings(qs_save = function(...) stop("no space left on device"), .package = "qs2")

  out <- collectMessages(objectsOnWorkers())
  expect_true(any(grepl("trying clusterExport", out$messages)))
  expect_length(out$value, 2L)
  for (w in out$value) expect_identical(w, c(a = 1, b = 2))
})
