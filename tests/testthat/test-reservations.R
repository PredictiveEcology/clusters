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

    first <- clusters:::.speedAllocate(nodes, total = 60, beta = 0.5)
    reserveCores(first)
    expect_equal(sum(first$assign), 60L)

    ## The second builder must now see only what is genuinely left (86 - 60 = 26).
    second <- clusters:::.speedAllocate(freeCoresLessReserved(nodes),
                                          total = 60, beta = 0.5)
    expect_equal(sum(second$assign), 26L)
    expect_lte(sum(first$assign) + sum(second$assign), sum(nodes$free_est))
  })
})

## Backdate every reservation in the ledger by `minutes`.
ageLedger <- function(minutes) {
  f <- reservationsPath()
  res <- readRDS(f)
  res$created <- res$created - minutes * 60
  saveRDS(res, f)
}

test_that("an old reservation is not subtracted again once the load average carries its load", {
  ## 2026-09-15, FireSense phase 2: fits built 1-3 h after other fits got 5, 2 and 7 of 100
  ## workers. free_est comes from the 5-min load average, which already carried the running
  ## fits' load, and their reservations were subtracted on top: every busy host counted twice.
  withLedger({
    reserveCores(data.frame(host = "a", assign = 8L))
    ageLedger(60)
    out <- freeCoresLessReserved(data.frame(host = "a", free_est = 10), loadWindowMinutes = 5)
    expect_equal(out$free_est, 10, tolerance = 0.01)
    expect_equal(out$reserved, 8L)   # still reported: the cores are in use
  })
})

test_that("a reservation is subtracted by the share of its load the average has not absorbed", {
  withLedger({
    reserveCores(data.frame(host = "a", assign = 8L))
    ageLedger(7)
    out <- freeCoresLessReserved(data.frame(host = "a", free_est = 10), loadWindowMinutes = 5,
                                 graceMinutes = 2)
    ## load starts after the 2-min grace; a 5-min damped average shows 1 - exp(-1) of it 5 min later
    expect_equal(out$free_est, 10 - 8 * exp(-1), tolerance = 0.01)
  })
})

test_that("a longer load window keeps subtracting a reservation for longer", {
  withLedger({
    reserveCores(data.frame(host = "a", assign = 8L))
    ageLedger(7)
    out15 <- freeCoresLessReserved(data.frame(host = "a", free_est = 10), loadWindowMinutes = 15,
                                   graceMinutes = 2)
    expect_equal(out15$free_est, 10 - 8 * exp(-1 / 3), tolerance = 0.01)
  })
})

test_that("a reservation counts in full during the grace period, however slow the machine", {
  ## CI on Windows took 0.2 s between reserving and reading, and without a grace period the
  ## decay had already started: free_est was 6.003 instead of 6.
  withLedger({
    reserveCores(data.frame(host = "a", assign = 4L))
    ageLedger(1.5)
    out <- freeCoresLessReserved(data.frame(host = "a", free_est = 10), loadWindowMinutes = 5,
                                 graceMinutes = 2)
    expect_identical(out$free_est, 6)
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

test_that("the test suite never uses the real per-user ledger folder", {
  ## setup-userState.R points clusters.reservationsPath at a temporary folder for the whole suite
  real <- normalizePath(tools::R_user_dir("clusters", "data"), mustWork = FALSE)
  expect_false(is.null(getOption("clusters.reservationsPath")))
  expect_false(startsWith(normalizePath(reservationsPath(), mustWork = FALSE), real))
  expect_false(startsWith(normalizePath(clusters:::.hostSpeedFile(), mustWork = FALSE), real))
})

## 2026-10-02: a build decided where its workers went 9 s before the previous build booked the ones it had
## chosen minutes earlier, so both took the same cores and hosts with 48 threads got 49 workers. Two
## processes allocate at once here; the probe sleeps to stand in for the minutes between a probe and a
## booking. One of them gets the host's 4 cores; the other finds none.
test_that("two builds allocating at once never book more than a host has", {
  skip_on_os("windows")
  skip_on_cran()
  d <- withr::local_tempdir()
  withr::local_options(clusters.reservationsPath = d, clusters.keepFreeCores = 0)
  owner <- Sys.getpid()   # the children's pids die with them, and liveReservations() would drop their rows
  allocate <- function() {
    probe <- function() {
      n <- freeCoresLessReserved(data.frame(host = "a", cores_total = 4L, free_est = 4))
      Sys.sleep(1)
      n
    }
    tryCatch(sum(getFromNamespace(".allocateWhenAvailable", "clusters")(
      probe, total = 4, waitSeconds = 0, book = function(alloc) reserveCores(alloc, pid = owner))$assign),
      error = function(e) 0L)
  }
  jobs <- lapply(1:2, function(i) parallel::mcparallel(allocate()))
  got <- unlist(parallel::mccollect(jobs))
  expect_equal(sort(unname(got)), c(0, 4))
  expect_equal(sum(liveReservations()$workers), 4L)
})

test_that("every host keeps clusters.keepFreeCores cores for its other users", {
  withLedger({
    nodes <- data.frame(host = c("a", "b"), cores_total = c(48, 16), free_est = c(48, 16))
    expect_equal(freeCoresLessReserved(nodes)$free_est, c(46, 14))
    reserveCores(data.frame(host = "a", assign = 40L))
    expect_equal(freeCoresLessReserved(nodes)$free_est, c(6, 14))
    withr::local_options(clusters.keepFreeCores = 4)
    expect_equal(freeCoresLessReserved(nodes)$free_est, c(4, 12))
  })
})

## Machines that share a home folder share the default ledger (a Slurm cluster, 2026-10-28): a build on one
## node booked "localhost" = 40 workers and a build on another node waited for it to finish.

## Write ledger rows as another machine's process would: its nodename leads the id (`owner`), `pid` is a
## process there. The ledger's columns are those of every version of the package (0.0.85 appends rows of
## exactly these), and `owner = NULL` writes an id without a machine, as an older version does.
writeRows <- function(host, workers, owner, pid, id = basename(tempfile("resv"))) {
  if (!is.null(owner)) id <- paste0(owner, ":", id)
  rows <- data.frame(id = id, pid = as.integer(pid), host = host, workers = as.integer(workers),
                     created = Sys.time(), memGB = NA_real_, stringsAsFactors = FALSE)
  file <- reservationsPath()
  saveRDS(if (file.exists(file)) rbind(readRDS(file), rows) else rows, file)
  invisible(id)
}
otherMachine <- paste0(Sys.info()[["nodename"]], "-other")

test_that("another machine's \"localhost\" booking is not subtracted from this machine's \"localhost\"", {
  withLedger({
    writeRows("localhost", 40, owner = otherMachine, pid = Sys.getpid())
    nodes <- data.frame(host = "localhost", nodename = Sys.info()[["nodename"]], free_est = 50)
    out <- freeCoresLessReserved(nodes)
    expect_equal(out$reserved, 0L)
    expect_equal(out$free_est, 50)
    ## without the node's own name in the table, "localhost" is still this machine
    out <- freeCoresLessReserved(nodes[c("host", "free_est")])
    expect_equal(out$reserved, 0L)
  })
})

test_that("a booking is still counted against the machine that made it", {
  withLedger({
    writeRows("localhost", 40, owner = otherMachine, pid = Sys.getpid())
    writeRows("localhost", 4, owner = Sys.info()[["nodename"]], pid = Sys.getpid())
    ## here, "localhost" is the 4 booked by this machine
    out <- freeCoresLessReserved(data.frame(host = "localhost", free_est = 50))
    expect_equal(out$reserved, 4L)
    ## an ssh name that resolves to the other machine gets its 40 (and not this machine's 4)
    out <- freeCoresLessReserved(data.frame(host = "sshAlias", nodename = otherMachine, free_est = 50))
    expect_equal(out$reserved, 40L)
  })
})

test_that("a row owned by another machine is not pruned for a pid that is not alive here", {
  withLedger({
    writeRows("sshAlias", 8, owner = otherMachine, pid = 999999L)
    expect_equal(liveReservations()$workers, 8L)
    expect_equal(freeCoresLessReserved(data.frame(host = "sshAlias", free_est = 10))$reserved, 8L)
    ## and booking another does not prune it either
    reserveCores(data.frame(host = "x", assign = 1L))
    expect_equal(sort(liveReservations()$workers), c(1L, 8L))
  })
})

test_that("a row owned by this machine with a dead pid is still pruned", {
  withLedger({
    writeRows("a", 8, owner = Sys.info()[["nodename"]], pid = 999999L)
    expect_equal(NROW(liveReservations()), 0L)
  })
})

test_that("rows with an id that names no machine still work, as before", {
  withLedger({
    writeRows("a", 8, owner = NULL, pid = Sys.getpid())
    writeRows("a", 5, owner = NULL, pid = 999999L)
    old <- readRDS(reservationsPath())
    old$memGB <- NULL                       # older still: before memGB
    saveRDS(old, reservationsPath())
    live <- liveReservations()
    expect_equal(live$workers, 8L)          # the dead one is pruned as before
    expect_equal(freeCoresLessReserved(data.frame(host = "a", free_est = 10))$reserved, 8L)
    reserveCores(data.frame(host = "a", assign = 2L))
    expect_equal(sort(liveReservations()$workers), c(2L, 8L))
  })
})

test_that("the ledger keeps the columns every version writes, and old and new code append to each other's", {
  withLedger({
    cols <- c("id", "pid", "host", "workers", "created", "memGB")
    ## this version writes no new column ...
    id <- reserveCores(data.frame(host = "a", assign = 3L))
    expect_equal(names(readRDS(reservationsPath())), cols)
    expect_match(id, paste0("^", Sys.info()[["nodename"]], ":"))
    ## ... so 0.0.85's reserveCores() (an rbind of a row of those columns, a bare id) appends to it ...
    old <- readRDS(reservationsPath())
    saveRDS(rbind(old, data.frame(id = "resv1234", pid = Sys.getpid(), host = "b", workers = 4L,
                                  created = Sys.time(), memGB = 8, stringsAsFactors = FALSE)),
            reservationsPath())
    expect_equal(sort(liveReservations()$workers), c(3L, 4L))
    ## ... and this version appends to, re-books and releases a ledger that version wrote
    id2 <- reserveCores(data.frame(host = "c", assign = 2L))
    clusters:::.rebookCores("resv1234", data.frame(host = "b", assign = 5L))
    expect_equal(names(readRDS(reservationsPath())), cols)
    expect_equal(sort(liveReservations()$workers), c(2L, 3L, 5L))
    releaseCores(id = c(id, id2, "resv1234"))
    expect_equal(NROW(liveReservations()), 0L)
  })
})

test_that("re-booking keeps the reservation's id, and so its owner", {
  withLedger({
    id <- writeRows("a", 8, owner = otherMachine, pid = 999999L)
    clusters:::.rebookCores(id, data.frame(host = "a", assign = 6L))
    expect_equal(liveReservations()$id, id)
    expect_equal(liveReservations()$workers, 6L)
  })
})
