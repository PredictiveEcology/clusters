## Each evaluation's record says which host and process ran it, so slow workers can be identified
## (FireSense, 2026-10-01: 48-59% of DEoptim time was spent waiting for the slowest evaluation, from
## consistently slow worker positions, but the records did not name the workers).
## Local PSOCK workers only: no ssh, no network.

skip_on_cran()
skip_if_not_installed("DEoptim")

test_that("an evaluation run on a PSOCK worker records that worker's host and pid", {
  cl <- parallel::makeCluster(2)
  on.exit(parallel::stopCluster(cl), add = TRUE)
  wpid <- unlist(parallel::clusterCall(cl, Sys.getpid))
  whost <- unlist(parallel::clusterCall(cl, function() Sys.info()[["nodename"]]))
  out <- suppressWarnings(clusters:::.DEoptimChunk(
    fn = function(par) sum((par - 0.3)^2), lower = c(0, 0), upper = c(1, 1),
    control = list(NP = 8L, itermax = 2L, trace = FALSE, cluster = cl,
                   initialpop = matrix(seq(0.05, 0.95, length.out = 16), 8)),
    known = list(keys = NULL, vals = NULL), dotsList = list()))
  ev <- out$member$evaluations
  expect_gt(nrow(ev), 0L)
  expect_true(all(ev$pid %in% wpid))
  expect_setequal(ev$pid, wpid)
  expect_identical(ev$host, unname(whost[match(ev$pid, wpid)]))
  expect_false(Sys.getpid() %in% ev$pid)
})

test_that("workerSpeed() summarises a hand-built record per host", {
  mk <- function(sec, host, pid = 1L) data.frame(seconds = sec, value = 0, host = host, pid = pid)
  gen1 <- list(member = list(evaluations = mk(c(1, 1, 1, 1), "fast")))
  gen2 <- list(member = list(evaluations = rbind(mk(c(1, 1), "fast"), mk(c(2, 2, 2, 10), "slow", 7L))))
  old <- list(member = list(evaluations = data.frame(seconds = 5, value = 0)))  # no host: skipped
  res <- workerSpeed(list(gen1, gen2, old))
  expect_identical(res$host, c("slow", "fast"))            # slowest first
  expect_identical(res$n, c(4L, 6L))
  expect_equal(res$median, c(2, 1))
  expect_equal(res$p90, c(unname(quantile(c(2, 2, 2, 10), 0.9)), 1))
  expect_equal(res$ratio, c(2, 1) / median(c(1, 1, 1, 1, 1, 1, 2, 2, 2, 10)))   # overall median 1
  expect_equal(workerSpeed(list(gen1, gen2), by = "worker")$pid, c(7L, 1L))
  expect_error(workerSpeed(list(old)), "host")
})
