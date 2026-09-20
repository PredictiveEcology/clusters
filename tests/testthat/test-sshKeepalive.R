## The master must notice when a worker has died, rather than blocking on its socket.
##
## PSOCK workers are reached over SSH and the master reads results with unserialize() on the socket.
## That read is bounded by parallelly's `timeout`, deliberately set to 30 days so a long computation
## is never cut off. So when a worker's R process dies AFTER connecting, SSH holds the tunnel open,
## the socket never closes, and the master blocks effectively forever. `connectTimeout` does not
## help -- it bounds only the initial connect, which already succeeded.
##
## Observed 2026-09-19/20: two re-scores hung for hours after a worker died at startup with
## `Error in unserialize(node$con)` inside workRSOCK; the masters had to be killed by hand.
##
## The fix closes the door at the SSH layer via ServerAliveInterval/ServerAliveCountMax.

test_that(".sshKeepaliveOpts() sets both keepalive options", {
  o <- .sshKeepaliveOpts()
  expect_true(any(grepl("^ServerAliveInterval=", o)))
  expect_true(any(grepl("^ServerAliveCountMax=", o)))
})

test_that(".sshKeepaliveOpts() keeps the options it replaced", {
  o <- .sshKeepaliveOpts()
  expect_true("-T" %in% o)
  expect_true("ConnectTimeout=10" %in% o)
})

test_that(".sshKeepaliveOpts() bounds the wait to roughly two minutes", {
  o <- .sshKeepaliveOpts()
  interval <- as.numeric(sub("^ServerAliveInterval=", "", grep("^ServerAliveInterval=", o, value = TRUE)))
  countMax <- as.numeric(sub("^ServerAliveCountMax=", "", grep("^ServerAliveCountMax=", o, value = TRUE)))
  ## must be far below parallelly's 30-day socket timeout, and long enough not to trip on a busy worker
  expect_lt(interval * countMax, 300)
  expect_gt(interval * countMax, 30)
})

test_that(".sshKeepaliveOpts() appends extra options after its own", {
  o <- .sshKeepaliveOpts(c("-o", "ForwardX11=no"))
  expect_true("ForwardX11=no" %in% o)
  expect_true(any(grepl("^ServerAliveInterval=", o)))
  ## the extras come last, so a caller can override
  expect_equal(utils::tail(o, 2L), c("-o", "ForwardX11=no"))
})

test_that(".sshKeepaliveOpts() honours custom interval and count", {
  o <- .sshKeepaliveOpts(interval = 15L, countMax = 2L)
  expect_true("ServerAliveInterval=15" %in% o)
  expect_true("ServerAliveCountMax=2" %in% o)
})

test_that(".sshKeepaliveOpts() rejects nonsensical values", {
  expect_error(.sshKeepaliveOpts(interval = 0))
  expect_error(.sshKeepaliveOpts(countMax = 0))
  expect_error(.sshKeepaliveOpts(interval = c(10, 20)))
})

test_that("the options are well-formed for ssh: every -o is followed by a key=value", {
  o <- .sshKeepaliveOpts(c("-o", "ForwardX11=no"))
  flagAt <- which(o == "-o")
  expect_true(all(flagAt + 1L <= length(o)))
  expect_true(all(grepl("^[A-Za-z][A-Za-z0-9]*=[^=]+$", o[flagAt + 1L])))
})

test_that("plan_psock_min() and makeClusterPSOCK() both default to the keepalive options", {
  ## a regression guard: the two entry points drifted apart before -- clusterSetup's wrapper carried
  ## ForwardX11/ExitOnForwardFailure that estimateCluster's did not -- so assert both now pick up
  ## the shared default
  defaultFor <- function(fn) {
    f <- get(fn, envir = asNamespace("clusters"))
    paste(deparse(formals(f)$rshopts), collapse = " ")
  }
  expect_true(grepl("sshKeepaliveOpts", defaultFor("plan_psock_min")))
  expect_true(grepl("sshKeepaliveOpts", defaultFor("makeClusterPSOCK")))
})
