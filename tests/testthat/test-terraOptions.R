test_that("workers start at terra's defaults, and mirrorTerraOptions fixes that", {
  skip_on_cran()
  skip_if_not_installed("terra")

  ## _R_CHECK_LIMIT_CORES_ refuses more than 2 workers under R CMD check.
  n <- min(2L, parallel::detectCores())
  cl <- try(parallel::makeCluster(n), silent = TRUE)
  skip_if(inherits(cl, "try-error"), "cannot start a local cluster")
  on.exit(try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)

  orig <- terra::terraOptions(print = FALSE)
  on.exit(terra::terraOptions(memfrac = orig$memfrac, memmax = orig$memmax,
                              todisk = orig$todisk), add = TRUE)

  ## Deliberately unlike terra's defaults (0.5 / 16 / FALSE), so a worker reporting
  ## these can only have got them from the master.
  terra::terraOptions(memfrac = 0, memmax = 3, todisk = TRUE)

  read <- function() parallel::clusterEvalQ(cl, {
    o <- terra::terraOptions(print = FALSE)
    c(memfrac = o$memfrac, memmax = o$memmax, todisk = as.numeric(o$todisk))
  })

  ## The premise: a PSOCK worker is a fresh session and inherits nothing.
  before <- read()
  expect_equal(before[[1L]][["memfrac"]], 0.5)
  expect_equal(before[[1L]][["memmax"]], 16)
  expect_equal(before[[1L]][["todisk"]], 0)

  sent <- mirrorTerraOptions(cl)
  expect_equal(sent$memfrac, 0)
  expect_equal(sent$memmax, 3)
  expect_true(sent$todisk)

  after <- read()
  for (w in after) {
    expect_equal(w[["memfrac"]], 0)
    expect_equal(w[["memmax"]], 3)
    expect_equal(w[["todisk"]], 1)
  }
})

test_that("mirrorTerraOptions sends the settings, not the cluster", {
  ## FireSense, 2026-09-15: a fit stalled for hours in clusterSetup() after loading packages.
  ## The function sent to the workers was made inside mirrorTerraOptions(), so it carried `cl`;
  ## parallelly stores each node's call stack, which held the data given to do.call(runDEoptim, args).
  ## Every worker received 9.6 GB, one worker at a time.
  skip_on_cran()
  skip_if_not_installed("terra")
  cl <- try(do.call(function(big) parallelly::makeClusterPSOCK(1L, autoStop = TRUE),
                    list(big = runif(2e6))), silent = TRUE)
  skip_if(inherits(cl, "try-error"), "cannot start a local cluster")
  on.exit(try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)
  ## the premise: this cluster object is large
  expect_gt(length(serialize(cl, NULL)), 10 * 2^20)

  sent <- new.env()
  sent$bytes <- 0
  suppressMessages(trace("sendCall", where = asNamespace("parallel"), print = FALSE,
                         tracer = bquote(assign("bytes", .(sent)$bytes +
                                                  length(serialize(list(fun, args), NULL)), envir = .(sent)))))
  on.exit(suppressMessages(untrace("sendCall", where = asNamespace("parallel"))), add = TRUE)
  mirrorTerraOptions(cl)
  expect_gt(sent$bytes, 0)
  expect_lt(sent$bytes, 2^20)
})
