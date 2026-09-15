## Workers that all run on this machine start this R's Rscript, not whatever `Rscript` is first on PATH.
##
## clusters #12 CI, 2026-09-15: the Ubuntu R CMD check jobs never finished. R CMD check --as-cran puts a stub
## `Rscript` first on PATH that prints "'Rscript' should not be used without a path -- see par. 1.6 of the
## manual" and exits. plan_psock_min() started its workers with a bare "Rscript", so no worker of the
## object-move tests' local cluster started, and the build waited for them. Reproduced locally with
## rcmdcheck(args = "--as-cran").

thisR <- file.path(R.home("bin"), "Rscript")

test_that("a cluster on this machine only uses this R's Rscript", {
  expect_identical(clusters:::.localRscript("Rscript", "localhost"), thisR)
  expect_identical(clusters:::.localRscript("Rscript", c("localhost", "127.0.0.1")), thisR)
  expect_identical(clusters:::.localRscript("Rscript", Sys.info()[["nodename"]]), thisR)
})

test_that("remote hosts keep Rscript from their own PATH, and an explicit command is left alone", {
  expect_identical(clusters:::.localRscript("Rscript", c("localhost", "birds")), "Rscript")
  expect_identical(clusters:::.localRscript("/opt/R/4.6.1/bin/Rscript", "localhost"), "/opt/R/4.6.1/bin/Rscript")
})

test_that("with a stub Rscript first on PATH, plan_psock_min's workers still start this R", {
  skip_on_os("windows")   # the stub is a shell script
  stubDir <- withr::local_tempdir()
  stub <- file.path(stubDir, "Rscript")
  writeLines(c("#!/bin/sh", "echo \"'Rscript' should not be used without a path -- see par. 1.6 of the manual\"",
               "exit 1"), stub)
  Sys.chmod(stub, "755")
  withr::local_envvar(PATH = paste(stubDir, Sys.getenv("PATH"), sep = .Platform$path.sep))
  expect_identical(unname(Sys.which("Rscript")), stub)   # the premise: a bare Rscript is the stub

  ## what plan_psock_min passes to the first cluster it builds (the probe), without building it
  seen <- new.env()
  testthat::local_mocked_bindings(
    makeClusterPSOCK = function(workers, ..., rscript = NULL) {
      seen$rscript <- rscript
      stop("probe captured")
    },
    .package = "clusters")
  withr::local_options(clusters.reservationsPath = withr::local_tempfile(fileext = ".rds"))
  try(suppressMessages(clusters::plan_psock_min(hosts = "localhost", total = 2L, pkgsNeeded = "stats",
                                                libPath = .libPaths(), build_final_cluster = FALSE)),
      silent = TRUE)
  expect_false(is.null(seen$rscript))
  expect_identical(tail(seen$rscript, 1L), thisR)
  expect_false("Rscript" %in% seen$rscript)
})
