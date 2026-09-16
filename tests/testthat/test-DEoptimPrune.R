## The caller half of fireSenseUtils#61's `pruneAbove`.
##
## A DEoptim generation is synchronous: its wall time is the SLOWEST of its NP evaluations, not the
## median. Measured on ELF 4.1 (FireSense phase 2, 2026-09-16), p90 was 40.9 s against a max of
## 85.9 s. The objective function can stop an evaluation early once its partial score is already
## worse than any parent -- but only if it is TOLD what "worse than any parent" is, and only this
## function knows the population.
##
## `iterStep = 1` makes each generation its own DEoptim call, so between chunks the accepted
## population's values are in hand as `DE[[iter]]$member$popval` -- already extracted here to build
## `known`. That is exactly the bound: the worst value the current population would accept.
##
## MAX, NOT A QUANTILE, and this is the whole safety argument. DE compares each trial against ITS
## OWN parent, so a trial worse than the WORST parent is worse than its own parent and is certain to
## be rejected. Pruning it cannot change the search. A p90 bound would start discarding trials that
## beat their own parent -- i.e. good-but-slow trials -- which is precisely the failure this must
## avoid. The bound is on VALUE, never on elapsed time: a slow evaluation heading for a good score
## runs to completion.
##
## The 1e6 sentinels must be excluded. `.objfunSpreadFit()` returns failVal = 1e6 for any trial that
## already bailed, so a population containing even one sentinel would otherwise push the bound to
## 1e6 and prune nothing at all.

test_that(".prunePopulationBound() is the worst value the population would accept", {
  expect_identical(.prunePopulationBound(c(10, 55, 32)), 55)
})

test_that(".prunePopulationBound() excludes the failVal sentinels", {
  ## one bailed trial must not raise the bound to 1e6, which would disable pruning entirely
  expect_identical(.prunePopulationBound(c(10, 55, 1e6)), 55)
  expect_identical(.prunePopulationBound(c(1e6, 1e6, 42)), 42)
})

test_that(".prunePopulationBound() returns Inf when there is nothing to bound with", {
  ## Inf is the no-op: fireSenseUtils' `min(thresh * numYrsDone, pruneAbove)` then reduces to the
  ## static threshold, i.e. exactly the pre-existing behaviour. Never guess a bound.
  expect_identical(.prunePopulationBound(numeric(0)), Inf)
  expect_identical(.prunePopulationBound(NULL), Inf)
  expect_identical(.prunePopulationBound(c(1e6, 1e6)), Inf)
  expect_identical(.prunePopulationBound(c(NA_real_, NA_real_)), Inf)
  expect_identical(.prunePopulationBound(c(Inf, Inf)), Inf)
})

test_that(".prunePopulationBound() ignores NA and non-finite members", {
  ## a generation cached before values were kept yields NA popval; it must not poison the bound
  expect_identical(.prunePopulationBound(c(10, NA, 55)), 55)
  expect_identical(.prunePopulationBound(c(10, Inf, 55)), 55)
})

test_that(".fnTakesPruneAbove() is TRUE only for a function that can receive the argument", {
  ## `fn` is user-supplied. DEoptim passes the extra arguments straight through to it, so sending
  ## `pruneAbove` to a function that declares neither it nor `...` is an "unused argument" error --
  ## i.e. this would break every existing caller whose objective function is not
  ## fireSenseUtils::.objfunSpreadFit. Send the bound only where it can be received.
  expect_true(.fnTakesPruneAbove(function(par, pruneAbove = Inf) NULL))
  expect_true(.fnTakesPruneAbove(function(par, ...) NULL))
  expect_false(.fnTakesPruneAbove(function(par) NULL))
  expect_false(.fnTakesPruneAbove(function(par, thresh = 550) NULL))
})

test_that("DEoptimIterative2() passes the bound to the objective function as pruneAbove", {
  ## Parsed, not run: a real call needs a cluster. `dotsList` is the objective function's argument
  ## list, and it is in `omitArgs`, so threading the bound through it cannot invalidate a cached
  ## chunk -- the bound changes every generation and must not become part of the chunk's key.
  src <- paste(deparse(DEoptimIterative2), collapse = "\n")
  expect_match(src, "pruneAbove")
  expect_match(src, "\\.prunePopulationBound\\(popval\\)")
})
