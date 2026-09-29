## DEoptimIterative2() progress plots.
##
## 2026-09-15, FireSense phase 2: no DEoptim progress plots or "Saved:" messages appeared, although
## the 2026-09-08 fits made them every iteration. Plotting ran only when reproducible::isUpdated()
## said the generation had just been computed by Cache(). Under spades.useCache = "eventsOnly" the
## nested Cache() is skipped, isUpdated() is FALSE for every generation, and nothing was plotted.
## Plots must follow whether this session computed the generation (not replayed it from the cache),
## thinned to every `iterStep` generations plus the last one (Eliot: every 5 is fine, but they must be
## visible).

skip_if_not_installed("DEoptim")
## the progress plots are SpaDES.core::Plots(), a suggested package
skip_if_not_installed("SpaDES.core")

lower <- c(a = 0, b = 0, c = 0)
upper <- c(a = 1, b = 1, c = 1)

## Plot calls recorded as the iteration each one belongs to; the plotting helpers are mocked so no
## figure is drawn.
runPlotted <- function(itermax, cachePath, iterStep, plotted, cacheIterations = TRUE, plotEvery = 1L) {
  fn <- function(par) sum((par - 0.3)^2)
  withr::local_options(reproducible.cachePath = cachePath,
                       reproducible.useCache = FALSE,       # what spades.useCache = "eventsOnly" does
                       clusters.cacheDEoptimIterations = cacheIterations)
  ## Plots() is SpaDES.core's; record the calls instead of drawing.
  testthat::local_mocked_bindings(
    Plots = function(...) { plotted$calls <- plotted$calls + 1L; invisible(NULL) },
    .package = "SpaDES.core")
  testthat::local_mocked_bindings(
    visualizeDEoptimLines = function(...) NULL,
    visualizeDE = function(...) NULL,
    .package = "clusters")
  msgs <- testthat::capture_messages(suppressWarnings(
    clusters:::DEoptimIterative2(fn, lower = lower, upper = upper,
                                 control = list(NP = 8L, strategy = 2L, itermax = itermax, trace = FALSE),
                                 iterStep = iterStep, plotEvery = plotEvery,
                                 figurePath = withr::local_tempdir(), .plots = "png", cachePath = cachePath,
                                 runName = "plots", .verbose = -1)))
  plotted$iterations <- as.integer(sub(".*Plotting DEoptim progress at iteration ([0-9]+).*", "\\1",
                                       grep("Plotting DEoptim progress at iteration", msgs, value = TRUE)))
  invisible(plotted)
}

newPlotted <- function() { p <- new.env(); p$calls <- 0L; p$iterations <- integer(0); p }

test_that("with plotEvery = 1, progress is plotted every iterStep generations and at the last one, with nested caching off", {
  plotted <- newPlotted()
  runPlotted(itermax = 5, cachePath = withr::local_tempdir(), iterStep = 2, plotted = plotted)
  expect_identical(plotted$iterations, c(2L, 4L, 5L))
  expect_gt(plotted$calls, 0L)
})

test_that("generations replayed from the cache are not plotted again, except the final figures", {
  cp <- withr::local_tempdir()
  runPlotted(itermax = 4, cachePath = cp, iterStep = 2, plotted = newPlotted())
  again <- newPlotted()
  runPlotted(itermax = 4, cachePath = cp, iterStep = 2, plotted = again)
  expect_identical(again$iterations, 4L)
  expect_identical(again$calls, 6L)
})

test_that("progress is plotted when per-generation caching is turned off too", {
  plotted <- newPlotted()
  runPlotted(itermax = 3, cachePath = withr::local_tempdir(), iterStep = 1, plotted = plotted,
             cacheIterations = FALSE)
  expect_identical(plotted$iterations, 1:3)
})

## 2026-09-28: drawing the six figures after every generation took 8.3 s of each 53 s generation (16%
## of the wall time) in a FireSense fit, with all workers idle. Eliot approved plotting every 25.
test_that("plotEvery = 25 plots at generations 25 and 50 and at the final one", {
  plotted <- newPlotted()
  runPlotted(itermax = 60, cachePath = withr::local_tempdir(), iterStep = 1, plotted = plotted,
             plotEvery = 25L)
  expect_identical(plotted$iterations, c(25L, 50L, 60L))
  expect_identical(plotted$calls, 3L * 6L)
})

test_that("plotEvery counts generations when a chunk holds several", {
  plotted <- newPlotted()
  runPlotted(itermax = 13, cachePath = withr::local_tempdir(), iterStep = 3, plotted = plotted,
             plotEvery = 5L)
  ## chunks end at 3, 6, 9, 12, 13: those reaching 5 and 10, and the last
  expect_identical(plotted$iterations, c(6L, 12L, 13L))
})

test_that("plotEvery defaults to 25", {
  expect_identical(formals(clusters:::DEoptimIterative2)$plotEvery, 25L)
})
