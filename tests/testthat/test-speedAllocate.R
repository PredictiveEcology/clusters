## Where a cluster's workers go (2026-10-02). A worker runs at full speed while its host's workers are no
## more than its memory modules and physical cores; a generation waits for its slowest worker. Hosts with
## 6 modules (coco, fire) were left out as "slow" although they run at full speed up to about 6 workers.

host <- function(name, dimms, physical = 24, threads = 48, free = threads)
  data.frame(host = name, cores_total = threads, cores_physical = physical, dimms = dimms, free_est = free)

test_that("worker speed: full to the memory modules and physical cores, then dimms/n and shared cores", {
  sp <- getFromNamespace(".workerSpeed", "clusters")
  expect_equal(sp(c(1, 6, 12, 48), dimms = 6, physical = 24, threads = 48, beta = 0.5, memExponent = 1),
               c(1, 1, 0.5, 0.125))
  ## the calibrated default (2026-10-02): a gentle decline past the modules
  expect_equal(sp(48, dimms = 8, physical = 24, threads = 48, beta = 0.75), min((8 / 48)^0.2, (24 + 0.75 * 24) / 48))
  expect_equal(sp(36, dimms = NA, physical = 24, threads = 48, beta = 0.5), (24 + 0.5 * 12) / 36)
  expect_equal(sp(30, dimms = 16, physical = 24, threads = 48, beta = 0.5, memExponent = 1), 16 / 30)  # memory is the lower
  expect_equal(sp(30, dimms = 16, physical = 24, threads = 48, beta = 0.5, memExponent = 0.5), sqrt(16 / 30))
})

test_that("every host is filled to its memory modules before any goes past its own", {
  nodes <- rbind(host("coco", 6), host("camas", 16), host("dougfir", 8))
  a <- clusters:::.speedAllocate(nodes, total = 30, beta = 0.5)
  expect_equal(a$assign, c(6L, 16L, 8L))
  expect_equal(a$speed, c(1, 1, 1))
})

test_that("past their modules hosts are evened out, so a host with few modules still gets more than them", {
  nodes <- rbind(host("coco", 6), host("camas", 16), host("dougfir", 8))
  a <- clusters:::.speedAllocate(nodes, total = 60, beta = 0.5)
  expect_equal(sum(a$assign), 60L)
  expect_gt(a$assign[1], 6L)
  ## no host could take one of another's workers and leave the slowest faster
  expect_lt(diff(range(a$speed)), 0.1)
})

test_that("workers already on a host (other clusters', other load) count against its speed", {
  nodes <- rbind(host("a", 16, free = 48), host("b", 16, free = 38))   # b has 10 already
  a <- clusters:::.speedAllocate(nodes, total = 16, beta = 0.5)
  expect_equal(a$busy, c(0, 10))
  expect_true(all(a$assign + a$busy <= 16))   # both stay within their 16 modules: every worker at full speed
  expect_equal(a$speed, c(1, 1))
  a <- clusters:::.speedAllocate(nodes, total = 26, beta = 0.5)
  expect_equal(a$assign + a$busy, c(18, 18))  # past the modules, the two hosts end level
})

test_that("a host never gets more than its free cores, and every free core is used when asked for", {
  set.seed(1)
  for (i in 1:100) {
    k <- sample(2:5, 1)
    cores <- sample(c(16, 32, 48, 64), k, replace = TRUE)
    free <- pmin(sample(1:60, k, replace = TRUE), cores)
    nodes <- data.frame(host = letters[seq_len(k)], cores_total = cores, free_est = free,
                        dimms = sample(c(NA, 6, 8, 16), k, replace = TRUE))
    expect_equal(clusters:::.speedAllocate(nodes, total = sum(free), beta = 0.5)$assign, as.integer(free))
    a <- clusters:::.speedAllocate(nodes, total = max(1, sum(free) %/% 2), beta = 0.5)
    expect_true(all(a$assign <= free))
  }
})
