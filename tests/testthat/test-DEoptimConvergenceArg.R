## The early-stop settings of DEoptimIterative() are an argument, so a caller can put them in its own
## cache key. They are NOT in the per-chunk cache keys: a fit that stopped early under a lax rule
## resumes from its cached generations, and continues, when rerun with a stricter rule.

skip_if_not_installed("DEoptim")

test_that("deoptimConvergence(): argument beats option beats default", {
  withr::local_options(clusters.deoptimConvergenceWindow = NULL, clusters.deoptimMinGenerations = NULL,
                       clusters.deoptimConvergenceSE = NULL, clusters.deoptimConvergenceSDChange = NULL)
  expect_identical(deoptimConvergence(),
                   list(window = 300L, minGenerations = 350L, se = 1, sdChange = 0.1))
  withr::local_options(clusters.deoptimMinGenerations = 500L, clusters.deoptimConvergenceSE = 2)
  expect_identical(deoptimConvergence(),
                   list(window = 300L, minGenerations = 500L, se = 2, sdChange = 0.1))
  expect_identical(deoptimConvergence(list(minGenerations = 700L, sdChange = 0.5)),
                   list(window = 300L, minGenerations = 700L, se = 2, sdChange = 0.5))
  expect_error(deoptimConvergence(list(bogus = 1)), "bogus")
})

test_that("a fit stopped early resumes from its cached generations when rerun with a stricter rule", {
  lower <- c(a = 0, b = 0); upper <- c(a = 1, b = 1)
  calls <- new.env(); calls$n <- 0L
  fn <- function(par) { calls$n <- calls$n + 1L; sum((par - 0.3)^2) }
  ## lax rule: stop as soon as past minGenerations (the improvement and spread limits are huge)
  rule <- function(minGenerations) list(window = 2L, minGenerations = minGenerations, se = 1e9, sdChange = 1e9)
  fit <- function(cachePath, minGenerations) {
    calls$n <- 0L
    set.seed(1)
    withr::local_options(reproducible.cachePath = cachePath, reproducible.useCache = FALSE,
                         clusters.cacheDEoptimIterations = TRUE, clusters.deoptimRescoreEvery = 0L)
    DE <- suppressMessages(suppressWarnings(
      DEoptimIterative(fn, lower = lower, upper = upper,
                       control = list(NP = 8L, strategy = 2L, itermax = 30, trace = FALSE),
                       iterStep = 1L, figurePath = FALSE, progressFile = FALSE, .plots = NULL,
                       cachePath = cachePath, runName = "resume", .verbose = -1,
                       convergence = rule(minGenerations))))
    list(DE = DE, calls = calls$n)
  }
  cp <- withr::local_tempdir()
  lax <- fit(cp, minGenerations = 4L)
  expect_lt(length(lax$DE), 30)
  strict <- fit(cp, minGenerations = 8L)                            # same cache: resume
  fresh <- fit(withr::local_tempdir(), minGenerations = 8L)         # the stricter fit with no cache
  expect_gt(length(strict$DE), length(lax$DE))                      # it continued ...
  expect_identical(length(strict$DE), length(fresh$DE))
  ## ... evaluating only the new generations: NP = 8 objective calls each, where the cache-less fit needs all
  expect_identical(strict$calls, 8L * (length(strict$DE) - length(lax$DE)))
  expect_lt(strict$calls, fresh$calls)
})
