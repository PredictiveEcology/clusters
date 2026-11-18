## requestedWorkers() is the number of workers clusterSetup() asks the planner for (its `total`), so a caller
## that needs the population size before the cluster exists (fireSense_spreadFit) has it. These pin the rule
## clusterSetup() had inline: a number is that many "localhost", a repeated host is its count, else nCoresNeeded.

test_that("a number of cores is that many workers, whatever nCoresNeeded says; 1L is one host", {
  withr::local_options(clusters.maxWorkersPerHost = NULL)
  expect_identical(requestedWorkers(40L, 25), 40L)
  expect_identical(requestedWorkers(12, 100), 12L)
  expect_identical(requestedWorkers(1L, 25), 25L)
})

test_that("distinct host names give nCoresNeeded; a repeated host gives the number of names", {
  withr::local_options(clusters.maxWorkersPerHost = NULL)
  expect_identical(requestedWorkers(c("hostA", "hostB"), 25), 25L)
  expect_identical(requestedWorkers("hostA", 25), 25L)
  expect_identical(requestedWorkers(rep("hostA", 40), 25), 40L)
  expect_identical(requestedWorkers(c(rep("hostA", 3), "hostB"), 25), 4L)
})

test_that("NULL cores is no cluster: nCoresNeeded", {
  expect_identical(requestedWorkers(NULL, 30), 30L)
})

test_that("options(clusters.maxWorkersPerHost) cuts the count as clusterSetup() does", {
  withr::local_options(clusters.maxWorkersPerHost = c(hostA = 10))
  expect_message(n <- requestedWorkers(rep("hostA", 40), 25), "hostA cut by 30 to 10")
  expect_identical(n, 10L)
  expect_identical(requestedWorkers(c(rep("hostA", 40), rep("hostB", 5)), 25), 15L)
  ## cut to one worker per host: no repeat left, so nCoresNeeded
  withr::local_options(clusters.maxWorkersPerHost = 1)
  expect_identical(suppressMessages(requestedWorkers(rep("hostA", 40), 25)), 25L)
  ## an unnamed cap on a number of cores
  withr::local_options(clusters.maxWorkersPerHost = 8)
  expect_identical(suppressMessages(requestedWorkers(20L, 25)), 8L)
})

test_that("clusterSetup() builds the workers requestedWorkers() says (the planner's total)", {
  withr::local_options(clusters.maxWorkersPerHost = c(hostA = 10))
  seen <- NULL
  testthat::local_mocked_bindings(
    plan_psock_min = function(hosts, total, ...) { seen <<- list(hosts = hosts, total = total); stop("planned") },
    .package = "clusters")
  for (i in 1:3) {
    cores <- list(rep("hostA", 40), 7L, c("hostA", "hostB"))[[i]]
    seen <- NULL
    suppressMessages(try(clusterSetup(cores = cores, nCoresNeeded = 25, logPath = tempdir(), libPath = .libPaths()[1],
                                      objsNeeded = character(), pkgsNeeded = character()), silent = TRUE))
    expect_identical(as.integer(seen$total), c(10L, 7L, 25L)[i])
    expect_identical(suppressMessages(requestedWorkers(cores, 25)), c(10L, 7L, 25L)[i])
  }
})
