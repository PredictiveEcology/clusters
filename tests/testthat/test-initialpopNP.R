## clusterSetup() sets NP to the number of workers the cluster gets, which the caller cannot know in
## advance, while DEoptim insists that `initialpop` has exactly NP rows. A caller that supplies a
## starting population (e.g. parameter sets the objective is known to accept) gives at least as many
## rows as it might get workers, and the first NP are used.

test_that("an initialpop with more rows than NP is cut to NP, with a message", {
  pop <- matrix(seq_len(30), nrow = 10, ncol = 3)
  expect_message(out <- .fitInitialpopToNP(list(NP = 6L, initialpop = pop)), "using the first 6")
  expect_identical(out$initialpop, pop[1:6, ])
  expect_identical(out$NP, 6L)
})

test_that("exactly NP rows pass unchanged; fewer is an error; no NP takes the rows", {
  pop <- matrix(seq_len(18), nrow = 6, ncol = 3)
  expect_silent(out <- .fitInitialpopToNP(list(NP = 6L, initialpop = pop)))
  expect_identical(out$initialpop, pop)
  expect_error(.fitInitialpopToNP(list(NP = 8L, initialpop = pop)), "6 rows but the population .* is 8")
  expect_identical(.fitInitialpopToNP(list(NP = NULL, initialpop = pop))$NP, 6L)
  expect_identical(.fitInitialpopToNP(list(NP = NA, initialpop = pop))$NP, 6L)
  expect_identical(.fitInitialpopToNP(list(NP = 8L)), list(NP = 8L))
  expect_error(.fitInitialpopToNP(list(NP = 8L, initialpop = 1:3)), "must be a matrix")
})

test_that("clusterSetup() applies it to a starting population given in controlArgs or initialpop", {
  pop <- matrix(runif(30), nrow = 10, ncol = 3)
  ## no cluster (cores = NULL): NP is what the caller said, and initialpop is cut to it. Without a
  ## cluster clusterSetup() puts objsNeeded in the global environment; tidy that up.
  withr::defer(if (exists("pop", envir = .GlobalEnv, inherits = FALSE)) rm("pop", envir = .GlobalEnv))
  expect_message(
    ctrl <- clusterSetup(messagePrefix = "pop", itermax = 1, trace = FALSE, cores = NULL, NP = 4L,
                         logPath = withr::local_tempdir(), libPath = .libPaths(),
                         pkgsNeeded = "stats", objsNeeded = list("pop"), envir = environment(), initialpop = pop),
    "using the first 4")
  expect_identical(dim(ctrl$initialpop), c(4L, 3L))
  ctrl <- suppressMessages(
    clusterSetup(messagePrefix = "pop", itermax = 1, trace = FALSE, cores = NULL, NP = 4L,
                 logPath = withr::local_tempdir(), libPath = .libPaths(),
                 pkgsNeeded = "stats", objsNeeded = list("pop"), envir = environment(),
                 controlArgs = list(initialpop = pop)))
  expect_identical(dim(ctrl$initialpop), c(4L, 3L))
})
