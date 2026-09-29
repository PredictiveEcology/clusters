# Extracted from test-DEoptimShippedDataKey.R:53

# prequel ----------------------------------------------------------------------
skip_if_not_installed("DEoptim")
lower <- c(a = 0, b = 0)
upper <- c(a = 1, b = 1)
fn <- function(par) sum((par - target)^2)
fitWith <- function(target, cachePath) {
  assign("target", target, envir = .GlobalEnv)
  withr::defer(rm("target", envir = .GlobalEnv))
  control <- clusters::clusterSetup(cores = NULL, objsNeeded = "target", envir = environment(), logPath = tempfile(),
                                    itermax = 6, strategy = 2L, NP = 8L, trace = FALSE)
  withr::local_options(reproducible.cachePath = cachePath, clusters.cacheDEoptimIterations = TRUE)
  set.seed(1) # the same starting population for both fits, as two folds of one ELF have
  DE <- suppressMessages(suppressWarnings(
    clusters::DEoptimIterative(fn, lower = lower, upper = upper, control = control,
                                figurePath = FALSE, cachePath = cachePath, runName = "fold",
                                .verbose = -1)))
  DE[[length(DE)]]$optim$bestmem
}

# test -------------------------------------------------------------------------
cp <- withr::local_tempdir()
best1 <- fitWith(c(0.2, 0.2), cp)
best2 <- fitWith(c(0.8, 0.8), cp)
expect_true(all(abs(best1 - 0.2) < abs(best1 - 0.8)))
expect_true(all(abs(best2 - 0.8) < abs(best2 - 0.2)))
n <- nrow(reproducible::showCache(cp, verbose = -2))
expect_equal(fitWith(c(0.2, 0.2), cp), best1)
expect_identical(nrow(reproducible::showCache(cp, verbose = -2)), n)
