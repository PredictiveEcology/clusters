## The allocator weights a host's real cores fully and its hyperthreads by `beta`. It used to take every
## host's real cores as cores_total / 2, so a 24-core machine WITHOUT hyperthreading (FireSense: landr,
## kodama) was weighted as 12 real + 12 hyperthreads and got a quarter less than its share.

fakeSysfs <- function(siblings) {
  d <- withr::local_tempdir(.local_envir = parent.frame())
  for (i in seq_along(siblings)) {
    td <- file.path(d, paste0("cpu", i - 1L), "topology")
    dir.create(td, recursive = TRUE)
    writeLines(siblings[[i]], file.path(td, "thread_siblings_list"))
  }
  d
}

test_that("physical cores are counted from the Linux CPU topology", {
  skip_if_not(identical(Sys.info()[["sysname"]], "Linux"))
  ## 4 logical CPUs on 2 cores (hyperthreaded) and 4 logical CPUs on 4 cores (not)
  expect_identical(clusters:::.physicalCores(sysfs = fakeSysfs(c("0,2", "1,3", "0,2", "1,3"))), 2L)
  expect_identical(clusters:::.physicalCores(sysfs = fakeSysfs(c("0", "1", "2", "3"))), 4L)
})

test_that("/proc/cpuinfo is the fallback when the topology directory is absent", {
  skip_if_not(identical(Sys.info()[["sysname"]], "Linux"))
  ci <- withr::local_tempfile()
  writeLines(c("processor\t: 0", "physical id\t: 0", "core id\t\t: 0",
               "processor\t: 1", "physical id\t: 0", "core id\t\t: 1",
               "processor\t: 2", "physical id\t: 0", "core id\t\t: 0",
               "processor\t: 3", "physical id\t: 0", "core id\t\t: 1"), ci)
  expect_identical(clusters:::.physicalCores(sysfs = withr::local_tempdir(), cpuinfo = ci), 2L)
  expect_identical(clusters:::.physicalCores(sysfs = withr::local_tempdir(), cpuinfo = tempfile()), NA_integer_)
})

test_that("this machine reports between half and all of its logical cores as physical", {
  p <- clusters:::.physicalCores()
  skip_if(is.na(p))
  expect_gte(p, 1L)
  expect_lte(p, parallel::detectCores())
})

test_that("available physical cores scale with the cores this session may use", {
  expect_identical(clusters:::.availablePhysicalCores(48, logical = 48, physical = 24), 24L)
  expect_identical(clusters:::.availablePhysicalCores(24, logical = 24, physical = 24), 24L)
  ## limited to 10 of a hyperthreaded 48: 5 cores' worth
  expect_identical(clusters:::.availablePhysicalCores(10, logical = 48, physical = 24), 5L)
  expect_identical(clusters:::.availablePhysicalCores(48, logical = 48, physical = NA_integer_), NA_integer_)
})

test_that("a host without hyperthreading counts all its cores as real", {
  nodes <- data.frame(host = c("ht48", "noHT24"), cores_total = c(48, 24), free_est = c(48, 24),
                      cores_physical = c(24, 24))
  a <- clusters:::.ht_allocate_min(nodes, total = 60, beta = 0.5)
  expect_equal(a$preHT_free, c(24, 24))
  expect_equal(a$weighted_free, c(36, 24))
  expect_equal(sum(a$assign), 60L)
  expect_equal(a$assign, c(36L, 24L))
})

test_that("without cores_physical the allocation is unchanged (two threads per core assumed)", {
  nodes <- data.frame(host = c("ht48", "noHT24"), cores_total = c(48, 24), free_est = c(48, 24))
  a <- clusters:::.ht_allocate_min(nodes, total = 60, beta = 0.5)
  expect_equal(a$preHT_free, c(24, 12))
  expect_equal(a$weighted_free, c(36, 18))
  nodes$cores_physical <- c(24, NA)            # one host that could not say
  b <- clusters:::.ht_allocate_min(nodes, total = 60, beta = 0.5)
  expect_equal(b$weighted_free, c(36, 18))
})
