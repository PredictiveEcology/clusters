## A DEoptim fit keeps a small progress file that DEoptimDashboard() reads (FireSense, 2026-10-08: the only
## way to see a fit's progress was the cache plus tmux panes, on the machine that ran it).

skip_if_not_installed("DEoptim")

progressCols <- c("time", "generation", "variable", "best", "q10", "median", "q90")

runFit <- function(cachePath, progressFile = NULL, itermax = 3, runName = "prog") {
  withr::local_options(reproducible.cachePath = cachePath, reproducible.useCache = FALSE,
                       clusters.cacheDEoptimIterations = TRUE, .local_envir = parent.frame())
  suppressMessages(withCallingHandlers(
    clusters:::DEoptimIterative(
      function(par) sum((par - 0.3)^2), lower = c(a = 0, b = 0), upper = c(a = 1, b = 1),
      control = list(NP = 8L, strategy = 2L, itermax = itermax, trace = FALSE), iterStep = 1,
      figurePath = FALSE, .plots = NULL, cachePath = cachePath, runName = runName, .verbose = -1,
      progressFile = progressFile),
    warning = function(w) if (!grepl("progress file", conditionMessage(w))) invokeRestart("muffleWarning")))
}

test_that("a fit writes its progress to DEoptimProgress_<runName>.csv in the working directory", {
  withr::local_dir(withr::local_tempdir())
  cp <- withr::local_tempdir()
  runFit(cp)
  expect_true(file.exists("DEoptimProgress_prog.csv"))
  p <- data.table::fread("DEoptimProgress_prog.csv")
  expect_identical(names(p), progressCols)
  expect_identical(p$variable, c(rep(c("objective", "a", "b"), 3), "FINISHED"))
  expect_identical(p$generation, c(rep(1:3, each = 3), 3L))
  expect_true(all(p$q10[-nrow(p)] <= p$median[-nrow(p)] & p$median[-nrow(p)] <= p$q90[-nrow(p)] |
                  p$variable[-nrow(p)] == "objective"))
  expect_true(all(is.na(p[p$variable == "FINISHED", c("best", "q10", "median", "q90")])))
  obj <- p[p$variable == "objective", ]
  expect_true(all(diff(obj$best) <= 0))              # the best member never gets worse

  ## replayed from the cache, the file has the same rows, not doubled
  runFit(cp)
  again <- data.table::fread("DEoptimProgress_prog.csv")
  expect_identical(again[, -"time"], p[, -"time"])
})

test_that("progressFile = FALSE writes nothing, and a path is used, with its folder made", {
  withr::local_dir(withr::local_tempdir())
  cp <- withr::local_tempdir()
  runFit(cp, progressFile = FALSE)
  expect_length(list.files(".", all.files = TRUE, no.. = TRUE, recursive = TRUE, pattern = "csv"), 0L)
  runFit(cp, progressFile = file.path("deep", "folder", "mine.csv"), itermax = 2)
  expect_identical(max(data.table::fread("deep/folder/mine.csv")$generation), 2L)
})

test_that("a progress file that cannot be written warns and the fit still finishes", {
  withr::local_dir(withr::local_tempdir())
  writeLines("", "blocker")                                  # a file where the folder should be
  out <- NULL
  expect_warning(out <- runFit(withr::local_tempdir(), progressFile = file.path("blocker", "p.csv")),
                 "progress file")
  expect_length(out, 3L)
})

test_that("DEoptimProgress reads status, labels and the per-generation tables", {
  root <- withr::local_tempdir()
  writeProgress(file.path(root, "ELF1", "DEoptimProgress_1.csv"), age = 60)
  writeProgress(file.path(root, "ELF2", "DEoptimProgress_fold2.csv"), finished = TRUE, age = 3600)
  writeProgress(file.path(root, "ELF3", "DEoptimProgress_1.csv"), age = 3600)
  writeProgress(file.path(root, "DEoptimProgress_1.csv"), age = 60)
  p <- DEoptimProgress(root)
  f <- p$fits[order(p$fits$label), ]
  expect_identical(f$label, c(".", "ELF1", "ELF2 fold2", "ELF3"))
  expect_identical(f$status, c("RUNNING", "RUNNING", "FINISHED", "STOPPED"))
  expect_identical(f$generation, rep(2L, 4))
  expect_equal(f$best, rep(4, 4))
  expect_equal(f$median, rep(7, 4))                 # the population median at the last generation, not the first
  expect_s3_class(f$started, "POSIXct")
  expect_s3_class(f$updated, "POSIXct")
  expect_identical(names(p$gens), c("fit", "generation", "best", "q10", "median"))
  expect_identical(names(p$params), c("fit", "generation", "param", "best", "q10", "median", "q90"))
  expect_identical(nrow(p$gens), 8L)
  expect_identical(unique(p$params$param), "a")
})

test_that("DEoptimProgress re-reads only files whose modification time changed", {
  root <- withr::local_tempdir()
  f <- writeProgress(file.path(root, "DEoptimProgress_1.csv"), age = 60)
  state <- new.env()
  expect_identical(DEoptimProgress(root, state = state)$fits$status, "RUNNING")
  writeProgress(f, finished = TRUE, age = 30)
  expect_identical(DEoptimProgress(root, state = state)$fits$status, "FINISHED")
  expect_identical(nrow(DEoptimProgress(withr::local_tempdir())$fits), 0L)
})

fourFits <- function() {
  root <- withr::local_tempdir(.local_envir = parent.frame())
  writeProgress(file.path(root, "run", "DEoptimProgress_1.csv"), age = 60)
  writeProgress(file.path(root, "stop", "DEoptimProgress_1.csv"), age = 3600)
  writeProgress(file.path(root, "done", "DEoptimProgress_1.csv"), finished = TRUE, age = 3600)
  writeProgressErrored(file.path(root, "err", "DEoptimProgress_1.csv"), age = 60)   # recent, still ERRORED
  root
}

test_that("an ERRORED row makes the status ERRORED, ahead of RUNNING and STOPPED, not a parameter", {
  root <- fourFits()
  p <- DEoptimProgress(root)
  f <- p$fits[order(p$fits$label), ]
  expect_identical(f$label, c("done", "err", "run", "stop"))
  expect_identical(f$status, c("FINISHED", "ERRORED", "RUNNING", "STOPPED"))
  expect_identical(unique(p$params$param), "a")
})

test_that("`which` keeps only those statuses, in any case, in fits, gens and params", {
  root <- fourFits()
  all <- DEoptimProgress(root)
  expect_identical(DEoptimProgress(root, which = c("running", "stopped", "finished", "errored"))$fits$status,
                   all$fits$status)
  for (w in list("running", "STOPPED", "Finished", "errored", c("running", "ERRORED"))) {
    p <- DEoptimProgress(root, which = w)
    expect_true(all(p$fits$status %in% toupper(w)))
    expect_setequal(p$fits$status, toupper(w))
    expect_setequal(p$gens$fit, p$fits$fit)
    expect_setequal(p$params$fit, p$fits$fit)
  }
  expect_identical(nrow(DEoptimProgress(root, which = "running", runningWithin = 1)$fits), 0L)
  expect_identical(nrow(DEoptimProgress(root, which = "stopped", runningWithin = 7200)$fits), 0L)
  expect_error(DEoptimProgress(root, which = "crashed"))
})

test_that("a fit whose objective errors ends its progress file with ERRORED; a finished one with FINISHED", {
  skip_if_not_installed("DEoptim")
  withr::local_dir(withr::local_tempdir())
  cp <- withr::local_tempdir()
  withr::local_options(reproducible.cachePath = cp, reproducible.useCache = FALSE,
                       clusters.cacheDEoptimIterations = TRUE)
  calls <- 0L
  fn <- function(par) { calls <<- calls + 1L; if (calls > 20L) stop("objective failed"); sum((par - 0.3)^2) }
  expect_error(suppressMessages(clusters:::DEoptimIterative(
    fn, lower = c(a = 0, b = 0), upper = c(a = 1, b = 1),
    control = list(NP = 8L, strategy = 2L, itermax = 5, trace = FALSE), iterStep = 1,
    figurePath = FALSE, .plots = NULL, cachePath = cp, runName = "err", .verbose = -1)), "objective failed")
  p <- data.table::fread("DEoptimProgress_err.csv")
  expect_identical(p$variable[nrow(p)], "ERRORED")
  expect_identical(sum(p$variable == "FINISHED"), 0L)
  expect_identical(DEoptimProgress(".")$fits$status, "ERRORED")
  expect_identical(nrow(DEoptimProgress(".", which = "running")$fits), 0L)
  expect_identical(nrow(DEoptimProgress(".", which = "stopped")$gens), 0L)
})

test_that("a fit reached through a symlinked folder inside `path` is listed once", {
  skip_on_os("windows")
  root <- withr::local_tempdir()
  writeProgress(file.path(root, "ELF1", "DEoptimProgress_1.csv"))
  writeProgress(file.path(root, "ELF2", "DEoptimProgress_1.csv"))
  dir.create(file.path(root, "links"))
  file.symlink(file.path(root, "ELF1"), file.path(root, "links", "ELF1"))
  state <- new.env()
  for (i in 1:2) {                                    # and again on a refresh with the same state
    p <- DEoptimProgress(root, state = state)
    expect_identical(sort(p$fits$label), c("ELF1", "ELF2"))
    expect_identical(nrow(p$gens), 4L)
    expect_identical(nrow(clusters:::.fitsData(p$fits)), 2L)
  }
})
