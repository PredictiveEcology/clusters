## options(clusters.maxWorkersPerHost): a hard cap on one cluster's workers on any host. The user wanted mega
## limited to 20 workers; the cap must hold at a build (.fitCapacity() -> .allocateWhenAvailable()), in a
## rebalance, and for a repeated-host `cores` vector. withLedger(), node() and fakeClock() are in helper-placement.R

capacity <- function(nodes, total = 100, ...)
  suppressMessages(clusters:::.fitCapacity(nodes, total = total, load_memory = "5min", ...))
allocate <- function(nodes, total = 100) clusters:::.speedAllocate(nodes, total = total, beta = 0.75)
bigHosts <- function(...) rbind(node("mega", threads = 80L), node("birds", threads = 80L), node("pinus", threads = 80L))
assigned <- function(a) stats::setNames(a$assign, a$host)

test_that("without the option no host is capped", {
  withLedger()
  a <- allocate(capacity(bigHosts()))
  expect_gt(max(a$assign), 20)
})

test_that("a named cap limits that host at a build, and the other hosts take the workers", {
  withLedger()
  withr::local_options(clusters.maxWorkersPerHost = c(mega = 20))
  a <- assigned(allocate(capacity(bigHosts())))
  expect_lte(a[["mega"]], 20)
  expect_equal(sum(a), 100)
})

test_that("an unnamed value caps every host, and a named value overrides it for its host", {
  withLedger()
  withr::local_options(clusters.maxWorkersPerHost = 25)
  a <- assigned(allocate(capacity(bigHosts())))
  expect_true(all(a <= 25))
  withr::local_options(clusters.maxWorkersPerHost = c(25, mega = 10))
  a <- assigned(allocate(capacity(bigHosts())))
  expect_lte(a[["mega"]], 10)
  expect_true(all(a[c("birds", "pinus")] <= 25))
})

test_that("a cap named for this machine applies to it when it is called localhost", {
  withLedger()
  testthat::local_mocked_bindings(.localAliases = function(...) "mega", .package = "clusters")
  withr::local_options(clusters.maxWorkersPerHost = c(mega = 20))
  a <- assigned(allocate(capacity(rbind(node("localhost", threads = 80L), node("birds", threads = 80L)))))
  expect_lte(a[["localhost"]], 20)
  expect_gt(a[["birds"]], 20)
})

test_that("a rebalance never moves workers onto a host past its cap", {
  withLedger()
  withr::local_options(clusters.maxWorkersPerHost = c(mega = 20))
  hosts <- c(rep("birds", 40), rep("pinus", 20))
  ## this cluster's 60 workers are all booked; mega is idle and would be the fastest host
  n <- capacity(bigHosts(), total = 60, own = table(hosts))
  target <- allocate(n, total = 60)
  expect_lte(assigned(target)[["mega"]], 20)
  m <- clusters:::.rebalanceMoves(hosts, assigned(target))
  expect_lte(sum(m$add == "mega"), 20)
})

test_that("a build waits-or-stops at once when the caps leave too few workers", {
  withLedger()
  withr::local_options(clusters.maxWorkersPerHost = 10)
  clock <- fakeClock()
  probe <- function() capacity(bigHosts())
  expect_error(suppressMessages(clusters:::.allocateWhenAvailable(probe, total = 40, waitSeconds = 3600,
                                                                  sleep = clock$sleep, now = clock$now)),
               "could never start")
  expect_equal(clock$sleeps(), 0L)
  a <- clusters:::.allocateWhenAvailable(probe, total = 30, waitSeconds = 3600, sleep = clock$sleep, now = clock$now)
  expect_true(all(a$assign <= 10))
})

test_that("a repeated-host cores vector is trimmed to the cap, with a message naming the host", {
  withr::local_options(clusters.maxWorkersPerHost = c(mega = 3))
  cores <- c(rep("mega", 5), rep("birds", 4))
  expect_message(out <- clusters:::.trimToHostCap(cores), "mega")
  expect_equal(table(out)[["mega"]], 3)
  expect_equal(table(out)[["birds"]], 4)
  withr::local_options(clusters.maxWorkersPerHost = NULL)
  expect_identical(clusters:::.trimToHostCap(cores), cores)
})
