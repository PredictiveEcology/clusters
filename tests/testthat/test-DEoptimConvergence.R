## The early-stopping test in DEoptimIterative2() decides when a fit has converged.
##
## FireSense phase 2 (2026-09-16) found it could never fire. `bestvalit` from DEoptim is a monotone
## STEP function, not a noisy trend: one fit (ELF 4.3) had SEVEN unique values across 456 generations,
## with runs of up to 141 identical values. The old rule regressed `val ~ iter` over 200-generation
## windows and required `all(tail(pvals, 2) > 0.1)`:
##
##   * a window containing a step gives p ~ 1e-44 .. 1e-61, which prints as 0.0000;
##   * a window that is perfectly flat -- which is exactly what convergence looks like -- makes
##     summary.lm() warn "essentially perfect fit: summary may be unreliable" and return a degenerate
##     p of 0.0848 (observed), which is BELOW the 0.1 gate.
##
## So the signature of convergence failed the test, and every fit ran to `itermax`: ELF 14.3 stopped at
## exactly generation 1000 after 14 h 12 min, having been flat for hundreds of generations.
##
## The replacement asks the question directly for a monotone series: has the best value improved in
## the last `noImproveFor` generations?

test_that("premise: a flat window gives a degenerate, platform-dependent p-value", {
  ## A flat window is what convergence looks like, and summary.lm() cannot describe it: the residuals
  ## are ~0, so it warns "essentially perfect fit" and returns a p that means nothing. The VALUE is not
  ## reproducible -- 0.0848 on the FireSense host, 0.122 on the CI Linux runners -- so the old gate,
  ## `all(tail(pvals, 2) > 0.1)`, fired or did not by luck of the floating point: at 0.122 it would have
  ## stopped, at 0.0848 it never could. Assert the degeneracy, never the constant.
  ## Even the "essentially perfect fit" WARNING is platform-dependent -- the Linux runners emit it,
  ## macOS and Windows do not -- so assert only the two numeric properties, which hold everywhere.
  flat <- data.frame(iter = seq_len(200), val = rep(58419.45, 200))
  s <- suppressWarnings(summary(stats::lm(val ~ iter, data = flat)))
  expect_lt(abs(s$coefficients[2, 1]), 1e-8)   # slope indistinguishable from zero
  expect_lt(s$sigma, 1e-6)                     # residual SE ~ 0: nothing for a p-value to describe
})

test_that("premise: a window containing one step looks highly significant", {
  stepped <- data.frame(iter = seq_len(200), val = c(rep(58500, 120), rep(58419.45, 80)))
  p <- summary(stats::lm(val ~ iter, data = stepped))$coefficients[2, 4]
  expect_lt(p, 1e-20)
})

## A SECOND replacement (FireSense, 2026-09-17): "the best value has not improved for 200 generations" was
## wrong in both directions. DE never re-scores a surviving member, so the best value is usually a lucky draw:
## eight converged fits were re-scored 10 times per member, and the best-value rule had stopped four while
## their populations were still clearly improving and run three for 170-560 generations after their
## populations had flattened. Each new lucky record restarted its count.
##
## The rule now asks whether the POPULATION is still improving: has the median value moved by less than one
## standard error of the median over the last `window` generations? Applied to the recorded populations of
## those eight fits it stops the three flat ones (at 438, 693 and 444) and none of the five still improving.

## `n` values around `centre`, spread like a population (sd ~ `sd`), without touching the RNG stream
pop <- function(centre, sd = 100, n = 60) centre + sd * stats::qnorm(stats::ppoints(n))

test_that(".deoptimPopulationConverged() is TRUE once the median moved less than one SE over the window", {
  ## SE of the median of 60 values with sd 100: 1.2533 * 100 / sqrt(60) = 16.2
  popvals <- c(lapply(1:200, function(g) pop(10000 - g)),  # improving for 200 generations...
               lapply(1:200, function(g) pop(9800 - g / 100)))  # ...then only 2 more in 200
  expect_true(.deoptimPopulationConverged(popvals, seq_along(popvals), window = 200L, minGenerations = 350L))
})

test_that(".deoptimPopulationConverged() is FALSE while the median still improves by more than one SE", {
  popvals <- lapply(1:400, function(g) pop(10000 - g / 5))  # 40 better over the last 200: ~2.5 SE
  expect_false(.deoptimPopulationConverged(popvals, seq_along(popvals), window = 200L, minGenerations = 350L))
})

test_that("a new lucky best does not hold the fit open when the population has stopped moving", {
  ## the case the best-value rule got wrong: the median is flat, but one member keeps setting records
  popvals <- lapply(1:400, function(g) { v <- pop(9800); v[1] <- 9000 - g; v })
  expect_true(.deoptimPopulationConverged(popvals, seq_along(popvals), window = 200L, minGenerations = 350L))
})

test_that("the tolerance scales with the population's spread, and with the SE multiplier", {
  popvals <- lapply(1:400, function(g) pop(10000 - g / 5, sd = 400))   # 40 over 200, SE ~65: under 1 SE
  expect_true(.deoptimPopulationConverged(popvals, seq_along(popvals), window = 200L, minGenerations = 350L))
  expect_false(.deoptimPopulationConverged(popvals, seq_along(popvals), window = 200L, minGenerations = 350L,
                                           seMultiplier = 0.5))
})

test_that(".deoptimPopulationConverged() is FALSE before minGenerations, however flat", {
  popvals <- lapply(1:300, function(g) pop(9800))
  expect_false(.deoptimPopulationConverged(popvals, seq_along(popvals), window = 200L, minGenerations = 350L))
})

test_that("failed trials and missing values are left out, and too little to judge is FALSE", {
  flat <- lapply(1:400, function(g) pop(9800))
  withFails <- lapply(flat, function(v) { v[1:10] <- 1e6; v })       # the fail sentinel is not a value
  expect_true(.deoptimPopulationConverged(withFails, seq_along(withFails), window = 200L, minGenerations = 350L))
  noValues <- lapply(1:400, function(g) rep(NA_real_, 60))            # generations cached before popval was kept
  expect_false(.deoptimPopulationConverged(noValues, seq_along(noValues), window = 200L, minGenerations = 350L))
  expect_false(.deoptimPopulationConverged(list(), integer(0), window = 200L, minGenerations = 350L))
  allFailed <- lapply(1:400, function(g) rep(1e6, 60))
  expect_false(.deoptimPopulationConverged(allFailed, seq_along(allFailed), window = 200L, minGenerations = 350L))
})

test_that("chunks of several generations are compared with the latest chunk at least `window` earlier", {
  ## iterStep = 25: one population per 25 generations
  gens <- seq(25L, 500L, by = 25L)
  popvals <- lapply(gens, function(g) pop(if (g <= 250) 10000 - g else 9750))
  expect_true(.deoptimPopulationConverged(popvals, gens, window = 200L, minGenerations = 350L))   # 500 vs 300
  expect_false(.deoptimPopulationConverged(popvals[1:16], gens[1:16], window = 200L, minGenerations = 350L))  # 400 vs 200
})

test_that(".deoptimPopulationConverged() takes its settings from options when not given", {
  popvals <- lapply(1:400, function(g) pop(if (g <= 150) 10000 - g else 9850))   # flat for the last 250
  withr::local_options(clusters.deoptimConvergenceWindow = 200L, clusters.deoptimMinGenerations = 350L,
                       clusters.deoptimConvergenceSE = 1)
  expect_true(.deoptimPopulationConverged(popvals, seq_along(popvals)))
  withr::local_options(clusters.deoptimConvergenceWindow = 300L)      # now the window reaches the improvement
  expect_false(.deoptimPopulationConverged(popvals, seq_along(popvals)))
})

test_that("DEoptimIterative2() decides convergence from the population, not from bestvalit", {
  src <- paste(deparse(DEoptimIterative2), collapse = "\n")
  expect_match(src, ".deoptimPopulationConverged(", fixed = TRUE)
  expect_false(grepl(".deoptimConverged(", src, fixed = TRUE))
})
