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

test_that("premise: a flat window's p-value falls below the old 0.1 gate", {
  ## the exact constant (0.0848) is an artefact of summary.lm() on a perfect fit; assert the property
  ## that matters -- it is under the gate -- so the test does not break on a different R version
  flat <- data.frame(iter = seq_len(200), val = rep(58419.45, 200))
  p <- suppressWarnings(summary(stats::lm(val ~ iter, data = flat))$coefficients[2, 4])
  expect_lt(p, 0.1)
  expect_false(all(c(p, p) > 0.1))    # the old rule, fed its own best case
})

test_that("premise: a window containing one step looks highly significant", {
  stepped <- data.frame(iter = seq_len(200), val = c(rep(58500, 120), rep(58419.45, 80)))
  p <- summary(stats::lm(val ~ iter, data = stepped))$coefficients[2, 4]
  expect_lt(p, 1e-20)
})

test_that(".deoptimConverged() is TRUE once the best value has not improved for noImproveFor generations", {
  bv <- c(seq(100, 61, length.out = 150), rep(60, 250))   # 400 generations; flat for the last 250
  expect_true(.deoptimConverged(bv, noImproveFor = 200L, minGenerations = 350L))
})

test_that(".deoptimConverged() is FALSE while an improvement falls inside the window", {
  bv <- c(rep(100, 300), rep(60, 100))                    # improved 100 generations ago
  expect_false(.deoptimConverged(bv, noImproveFor = 200L, minGenerations = 350L))
})

test_that(".deoptimConverged() is FALSE before minGenerations, however flat", {
  expect_false(.deoptimConverged(rep(60, 300), noImproveFor = 200L, minGenerations = 350L))
})

test_that(".deoptimConverged() handles degenerate series without erroring", {
  ## all-infinite is what DEoptim reports before any member is scored; the old code guarded this too
  expect_false(.deoptimConverged(rep(Inf, 400), noImproveFor = 200L, minGenerations = 350L))
  expect_false(.deoptimConverged(numeric(0), noImproveFor = 200L, minGenerations = 350L))
  expect_false(.deoptimConverged(c(5, 4, 3), noImproveFor = 200L, minGenerations = 350L))
  expect_false(.deoptimConverged(c(rep(NA_real_, 10), rep(60, 400)),
                                 noImproveFor = 200L, minGenerations = 350L))
})

test_that(".deoptimConverged() takes its thresholds from options when not given", {
  bv <- c(rep(100, 100), rep(60, 300))                    # flat for the last 300 of 400
  withr::local_options(clusters.deoptimNoImproveFor = 200L, clusters.deoptimMinGenerations = 350L)
  expect_true(.deoptimConverged(bv))
  withr::local_options(clusters.deoptimNoImproveFor = 400L)
  expect_false(.deoptimConverged(bv))
})
