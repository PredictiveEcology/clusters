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
    ## every library: under R CMD check .libPaths()[1] holds only this package, and the workers also
    ## need parallelly, qs2 and reproducible ("there is no package called 'parallelly'")
    logPath = withr::local_tempdir(), libPath = .libPaths(),
    objsNeeded = list("x1"), pkgsNeeded = "stats", nCoresNeeded = 2L, envir = environment())
  on.exit(try(parallel::stopCluster(control$cluster), silent = TRUE), add = TRUE)
  parallel::clusterEvalQ(control$cluster, get0("x1", envir = .GlobalEnv))
}

localClusterOptions <- function(env = parent.frame()) {
  withr::local_options(clusters.reservationsPath = withr::local_tempfile(fileext = ".rds", .local_envir = env),
                       clusters.waitForCores = 0, clusters.minWorkersFraction = 1, .local_envir = env)
  ## clusterSetup() refuses fewer than 4 workers (DEoptim's minimum population), but R CMD check
  ## allows 2 processes; these tests are about moving objects, not the population size.
  testthat::local_mocked_bindings(.clusterNP = function(NP, nWorkers) nWorkers, .package = "clusters", .env = env)
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
  expect_true(dir.exists(tempdir()))   # local workers removed the master's tempdir() with the transfer file
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

## Builds a cluster with clusterSetup(), rebuilds it as DEoptimIterative() does when a worker dies, and
## calls `after(rebuilt, old)` before returning; plan_psock_min()'s exit handler runs when this returns.
fitWithRebuild <- function(after, logPath) {
  x1 <- 1
  control <- clusters::clusterSetup(
    messagePrefix = "rebuild", itermax = 1, trace = FALSE, cores = "localhost",
    logPath = logPath, libPath = .libPaths(),
    objsNeeded = list("x1"), pkgsNeeded = "stats", nCoresNeeded = 2L, envir = environment())
  after(attr(control$cluster, "restartCluster")(control$cluster), control$cluster)
}

test_that("after a rebuild, the exit handler closes no connection that took the old cluster's numbers", {
  ## It stopped the cluster it built, whose connections the rebuild had closed; R gives their numbers
  ## to the next connections opened, and stopping by a stale number closes whatever has it
  skip_on_cran()
  skip_on_os(c("windows", "mac"))
  localClusterOptions()
  cons <- list()
  opened <- list()
  logPath <- withr::local_tempdir()
  suppressMessages(fitWithRebuild(function(rebuilt, old) {
    srv <- serverSocket(port <- sample(20000:30000, 1L))
    on.exit(close(srv))
    clusters:::.stopCluster(rebuilt)          # done with it, as its autoStop finalizer would do
    ## new socket connections (as a next cluster's would be), until two have the first cluster's numbers
    opened <<- unlist(lapply(1:5, function(i)
      list(socketConnection("localhost", port, open = "r+b", blocking = TRUE),
           socketAccept(srv, open = "r+b", blocking = TRUE))), recursive = FALSE)
    oldNumbers <- vapply(old, function(node) as.integer(node$con), integer(1))
    cons <<- opened[vapply(opened, as.integer, integer(1)) %in% oldNumbers]
  }, logPath))
  withr::defer(for (con in opened) try(close(con), silent = TRUE))
  expect_length(cons, 2L)
  expect_true(all(vapply(cons, function(con) isTRUE(tryCatch(isOpen(con), error = function(e) FALSE)),
                         logical(1))))
})

test_that("after a rebuild, the exit handler stops the rebuilt cluster", {
  skip_on_cran()
  skip_on_os(c("windows", "mac"))
  localClusterOptions()
  pids <- integer(0)
  logPath <- withr::local_tempdir()
  suppressMessages(fitWithRebuild(function(rebuilt, old)
    pids <<- unlist(parallel::clusterCall(rebuilt, Sys.getpid)), logPath))
  for (i in 1:50) if (any(clusters:::.pidAlive(pids))) Sys.sleep(0.1)
  expect_false(any(clusters:::.pidAlive(pids)))
})

## Free cores are capped by every worker booked on a host, so a stopped cluster's booking must go when it
## stops: left for R to collect, it counted against the next build in the same process (2026-10-02).
test_that("the cluster's reservation is released when the exit handler stops it", {
  skip_on_cran()
  skip_on_os(c("windows", "mac"))
  localClusterOptions()
  booked <- NULL
  suppressMessages(fitWithRebuild(function(rebuilt, old) booked <<- NROW(clusters::liveReservations()),
                                  withr::local_tempdir()))
  expect_gt(booked, 0L)
  expect_equal(NROW(clusters::liveReservations()), 0L)
})
