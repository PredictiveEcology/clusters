## Core reservations: the ledger that lets concurrent cluster builds see each
## other's claims instead of both trusting a trailing load average.

withLedger <- function(code) {
  d <- file.path(tempdir(), paste0("resv", sample(1e6, 1)))
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  withr::defer(unlink(d, recursive = TRUE), envir = parent.frame())
  old <- options(clusters.reservationsPath = d)
  withr::defer(options(old), envir = parent.frame())
  force(code)
  d
}

test_that("an empty ledger reserves nothing", {
  withLedger({
    expect_equal(NROW(liveReservations()), 0L)
    nodes <- data.frame(host = c("a", "b"), free_est = c(10, 20))
    out <- freeCoresLessReserved(nodes)
    expect_equal(out$free_est, c(10, 20))
    expect_equal(out$reserved, c(0L, 0L))
  })
})

test_that("a reservation is subtracted from the free cores of its host only", {
  withLedger({
    reserveCores(data.frame(host = c("a", "b"), assign = c(4L, 0L)))
    nodes <- data.frame(host = c("a", "b", "c"), free_est = c(10, 20, 30))
    out <- freeCoresLessReserved(nodes)
    ## host "b" was assigned 0 and must not be recorded at all
    expect_equal(out$reserved, c(4L, 0L, 0L))
    expect_equal(out$free_est, c(6, 20, 30))
  })
})

test_that("free cores floor at zero rather than going negative", {
  withLedger({
    reserveCores(data.frame(host = "a", assign = 99L))
    out <- freeCoresLessReserved(data.frame(host = "a", free_est = 10))
    expect_equal(out$free_est, 0)
  })
})

test_that("concurrent reservations accumulate, and release is per reservation", {
  withLedger({
    id1 <- reserveCores(data.frame(host = "a", assign = 30L))
    id2 <- reserveCores(data.frame(host = "a", assign = 25L))
    expect_false(identical(id1, id2))

    out <- freeCoresLessReserved(data.frame(host = "a", free_est = 100))
    expect_equal(out$reserved, 55L)

    ## Releasing one cluster must not release the other held by the same process.
    releaseCores(id = id1)
    out <- freeCoresLessReserved(data.frame(host = "a", free_est = 100))
    expect_equal(out$reserved, 25L)

    releaseCores(id = id2)
    expect_equal(NROW(liveReservations()), 0L)
  })
})

test_that("a reservation owned by a dead process is dropped on read", {
  withLedger({
    reserveCores(data.frame(host = "a", assign = 8L), pid = 999999L)
    ## pid 999999 does not exist, so the entry must not hold cores hostage.
    expect_equal(NROW(liveReservations()), 0L)
    out <- freeCoresLessReserved(data.frame(host = "a", free_est = 10))
    expect_equal(out$reserved, 0L)
    expect_equal(out$free_est, 10)
  })
})

test_that("this is what stops two builders double-booking the same cores", {
  ## The regression the ledger exists for: without it, two builds sized from the
  ## same freeCores() reading each take the full fleet.
  withLedger({
    nodes <- data.frame(host = c("n1", "n2"), cores_total = c(48, 48),
                        free_est = c(43, 43))

    first <- clusters:::.ht_allocate_min(nodes, total = 60, beta = 0.5)
    reserveCores(first)
    expect_equal(sum(first$assign), 60L)

    ## The second builder must now see only what is genuinely left (86 - 60 = 26).
    second <- clusters:::.ht_allocate_min(freeCoresLessReserved(nodes),
                                          total = 60, beta = 0.5)
    expect_equal(sum(second$assign), 26L)
    expect_lte(sum(first$assign) + sum(second$assign), sum(nodes$free_est))
  })
})

test_that(".pidAlive answers correctly on whatever platform is running the tests", {
  ## Deliberately not skipped anywhere: this is the check that failed silently
  ## on two platforms at once. /proc does not exist on macOS, where every pid
  ## looked dead; tasklist exits 0 even when its filter matches nothing, so on
  ## Windows every pid looked alive.
  expect_true(clusters:::.pidAlive(Sys.getpid()))
  expect_false(clusters:::.pidAlive(999999L))
  expect_false(clusters:::.pidAlive(NA_integer_))
  expect_equal(clusters:::.pidAlive(c(Sys.getpid(), 999999L)), c(TRUE, FALSE))
})
