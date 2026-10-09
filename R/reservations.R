#' Core reservations shared between concurrent cluster builds
#'
#' @description
#' `plan_psock_min()` sizes a cluster from [parallelly::freeCores()], which reads
#' a *trailing* load average (5 minutes by default). Two clusters built inside
#' that window therefore both observe the same idle cores and both claim them:
#' the load produced by the first has not yet reached the average the second
#' reads. Staggering the builds by more than the averaging window papers over
#' this; it does not fix it, and it forces callers to hand-tune both the stagger
#' and the number of concurrent jobs against the size of the fleet.
#'
#' These functions keep a small ledger of what has actually been handed out, so
#' allocation is based on *reservations* rather than on a lagging measurement.
#' Each successful build records `host -> workers` (and the memory those workers are expected to need,
#' `memGB`) under a reservation id owned by the building process; [freeCoresLessReserved()] subtracts those from the
#' measured free cores. Releases are keyed by id, because one process may hold
#' several clusters at once.
#'
#' The ledger is self-healing: every read drops entries whose owning process is
#' no longer alive, so a master that is killed without releasing cannot leak a
#' reservation forever.
#'
#' @section Scope:
#' The ledger is a file, so it is shared by every process that can see that file
#' -- which is what is needed when several worker panes on one machine each build
#' their own cluster over a shared fleet. If *different* machines build clusters
#' over the same fleet, point them all at one path on shared storage via
#' `options(clusters.reservationsPath = )`.
#'
#' @param path Directory holding the ledger. Defaults to
#'   `getOption("clusters.reservationsPath")`, else a per-user data directory.
#' @return `reservationsPath()` a file path; `liveReservations()` a data.frame
#'   with columns `id`, `pid`, `host`, `workers`, `created`, `memGB` (`NA` in rows
#'   written before the column existed); `reserveCores()`
#'   returns its reservation `id`; `releaseCores()` returns invisibly.
#' @rdname reservations
#' @export
reservationsPath <- function(path = getOption("clusters.reservationsPath")) {
  if (is.null(path))
    path <- file.path(tools::R_user_dir("clusters", "data"))
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  file.path(path, "coreReservations.rds")
}

# Serialise every read-modify-write through one lock; the whole point is that
# concurrent builders see each other.
.withReservationLock <- function(file, expr) {
  if (!requireNamespace("filelock", quietly = TRUE))
    stop("Package 'filelock' is required for core reservations. ",
         "Install it, or disable reservations with options(clusters.useReservations = FALSE).",
         call. = FALSE)
  lck <- filelock::lock(paste0(file, ".lock"), timeout = 60000L)
  if (is.null(lck))
    stop("Timed out waiting for the core-reservation lock: ", file, ".lock", call. = FALSE)
  on.exit(try(filelock::unlock(lck), silent = TRUE), add = TRUE)
  force(expr)
}

## One allocation decision at a time, across processes: a build's probe -> allocate -> book, or a running
## cluster's probe -> decide -> re-book (.rebalanceFn()). Without it two builds deciding within the same
## few minutes saw the same free cores and both took them (2026-10-02: a build decided at 13:01:36, 9 s
## before the previous build booked the workers it had chosen at 12:56:20, and hosts with 48 threads
## got 49 workers).
.withAllocationLock <- function(expr, path = getOption("clusters.reservationsPath"),
                                timeoutMinutes = getOption("clusters.allocationLockMinutes", 5)) {
  file <- file.path(dirname(reservationsPath(path)), "allocation.lock")
  lck <- filelock::lock(file, timeout = timeoutMinutes * 60 * 1000)
  if (is.null(lck))
    stop("another cluster held the allocation lock (", file, ") for ", timeoutMinutes, " minutes",
         call. = FALSE)
  on.exit(filelock::unlock(lck), add = TRUE)
  force(expr)
}

## Is this process still running? Wrong in either direction costs real work: a
## dead owner reported alive holds cores hostage until someone notices, and a
## live owner reported dead lets a second builder take cores that are in use.
##
## Both errors were live here. `/proc` exists on Linux but not on macOS, where
## every pid therefore looked dead. And `tasklist` exits 0 whether or not the
## filter matched -- it prints "No tasks are running which match" -- so on
## Windows every pid looked alive, including this test's deliberately dead one.
.pidAlive <- function(pid) {
  vapply(pid, function(p) {
    if (is.na(p)) return(FALSE)
    if (identical(.Platform$OS.type, "windows")) {
      out <- try(suppressWarnings(system2("tasklist", c("/NH", "/FI", shQuote(paste0("PID eq ", p))),
                                          stdout = TRUE, stderr = FALSE)), silent = TRUE)
      if (inherits(out, "try-error") || !length(out)) return(FALSE)
      ## The pid appears in the row only when the filter actually matched.
      return(any(grepl(paste0("(^|[^0-9])", p, "([^0-9]|$)"), out)))
    }
    if (dir.exists("/proc")) return(dir.exists(file.path("/proc", p)))
    ## macOS and the other BSDs: ask ps, whose exit status answers directly.
    identical(suppressWarnings(system2("ps", c("-p", p), stdout = FALSE, stderr = FALSE)), 0L)
  }, logical(1))
}

.emptyReservations <- function()
  data.frame(id = character(0), pid = integer(0), host = character(0),
             workers = integer(0), created = as.POSIXct(character(0)), memGB = numeric(0),
             stringsAsFactors = FALSE)

## The ledger as saved in `file`; rows written before `memGB` existed have it NA, so every reader and
## every rbind() sees the same columns.
.readReservations <- function(file) {
  res <- tryCatch(readRDS(file), error = function(e) .emptyReservations())
  if (is.data.frame(res) && !"memGB" %in% names(res)) res$memGB <- rep(NA_real_, NROW(res))
  res
}

## GB booked for `assign` workers at `memPerWorkerGB` each; NA when the memory per worker is not known
.bookedMemoryGB <- function(assign, memPerWorkerGB)
  as.numeric(assign) * (if (is.null(memPerWorkerGB)) NA_real_ else as.numeric(memPerWorkerGB)[1])

#' @rdname reservations
#' @export
liveReservations <- function(path = getOption("clusters.reservationsPath")) {
  file <- reservationsPath(path)
  if (!file.exists(file)) return(.emptyReservations())
  .withReservationLock(file, {
    res <- .readReservations(file)
    if (!NROW(res)) {
      .emptyReservations()
    } else {
      keep <- .pidAlive(res$pid)
      # A dead owner cannot still be using cores; drop it so the ledger cannot leak.
      if (!all(keep)) {
        res <- res[keep, , drop = FALSE]
        saveRDS(res, file)
      }
      res
    }
  })
}

#' @param alloc A data.frame with columns `host` and `assign`, as returned in the
#'   `allocation` element of [plan_psock_min()].
#' @param id Reservation id. One process may hold several clusters at once, so
#'   releases are keyed by reservation rather than by process.
#' @param pid Owning process id; defaults to this process.
#' @param memPerWorkerGB GB of memory each of the workers is expected to need, booked beside the cores
#'   (`NULL`, the default, books none).
#' @rdname reservations
#' @export
reserveCores <- function(alloc, id = basename(tempfile("resv")), pid = Sys.getpid(), memPerWorkerGB = NULL,
                         path = getOption("clusters.reservationsPath")) {
  stopifnot(all(c("host", "assign") %in% names(alloc)))
  alloc <- alloc[alloc$assign > 0, , drop = FALSE]
  if (!NROW(alloc)) return(invisible(id))
  file <- reservationsPath(path)
  .withReservationLock(file, {
    res <- if (file.exists(file))
      .readReservations(file) else .emptyReservations()
    res <- res[.pidAlive(res$pid), , drop = FALSE]
    res <- rbind(res, data.frame(id = id, pid = as.integer(pid), host = alloc$host,
                                 workers = as.integer(alloc$assign),
                                 created = Sys.time(), memGB = .bookedMemoryGB(alloc$assign, memPerWorkerGB),
                                 stringsAsFactors = FALSE))
    saveRDS(res, file)
  })
  invisible(id)
}

#' @rdname reservations
#' @export
releaseCores <- function(id = NULL, pid = Sys.getpid(),
                         path = getOption("clusters.reservationsPath")) {
  file <- reservationsPath(path)
  if (!file.exists(file)) return(invisible(NULL))
  .withReservationLock(file, {
    res <- .readReservations(file)
    if (NROW(res)) {
      drop <- if (is.null(id)) res$pid %in% pid else res$id %in% id
      saveRDS(res[!drop, , drop = FALSE], file)
    }
  })
  invisible(NULL)
}

#' Subtract live reservations from measured free cores
#'
#' @param nodes A data.frame with `host` and `free_est`, as built by
#'   [plan_psock_min()] from its probe.
#' @inheritParams reservations
#' @param path Path to the reservations ledger; defaults to `getOption("clusters.reservationsPath")`.
#' @param loadWindowMinutes The averaging window, in minutes, of the load average
#'   `free_est` was measured from (5 for `parallelly::freeCores(memory = "5min")`).
#' @param graceMinutes Minutes after it is made during which a reservation counts in
#'   full. A reservation is recorded when the cluster has started, but its workers
#'   only load the hosts once objects are copied to them and the work begins (2.5-4
#'   minutes for the FireSense spread fits).
#' @details `free_est` already contains the load of clusters that have been
#'   running for a while, so subtracting their whole reservation counts those cores
#'   twice. A load average is exponentially damped: `t` minutes after a cluster's
#'   load starts it shows `1 - exp(-t / loadWindowMinutes)` of it. Each reservation is
#'   therefore subtracted only by the share the average has not absorbed yet,
#'   `workers * exp(-max(age - graceMinutes, 0) / loadWindowMinutes)`: all of it for a
#'   cluster built moments ago, which is what stops concurrent builds double-booking,
#'   and almost none of it for a cluster that has been running for half an hour.
#'
#'   The load average is not a full account of a running cluster either: a DEoptim worker waits, idle,
#'   while each generation's slowest evaluation finishes, so a host carrying 50 booked workers showed a
#'   load of 30-35 (2026-10-02). Read as free, that gap was booked by every later build, until hosts
#'   with 48 threads carried 50-52 workers. So `free_est` is also capped at `cores_total` less every
#'   worker booked on the host, absorbed or not, and less `getOption("clusters.keepFreeCores", 2)` cores
#'   left for the host's other users, when `nodes` has `cores_total`.
#'
#'   Memory is booked the same way. When `nodes` has `mem_free_gb` (the host's `MemAvailable`), each
#'   reservation's `memGB` is subtracted by the same unabsorbed share: a new cluster's workers have not grown
#'   yet, so `MemAvailable` does not show the memory they will take, and a build deciding in those minutes
#'   would count it free. Rows without `memGB` (an older ledger) book none.
#' @param exclude Reservation ids not to count: a cluster asking where its own workers should be
#'   counts every cluster but itself (see [.rebalanceFn()]).
#' @return `nodes` with `free_est` reduced by the unabsorbed share of every live
#'   reservation on that host and capped at `cores_total` less all of them, floored at zero, plus a
#'   `reserved` column (the reserved workers); `mem_free_gb`, when present, reduced the same way, floored
#'   at zero, with `reserved_gb`, the memory booked. Reservations held by this process count
#'   too: a master that already built one cluster is genuinely using those cores
#'   while it builds the next.
#' @export
freeCoresLessReserved <- function(nodes,
                                  path = getOption("clusters.reservationsPath"),
                                  loadWindowMinutes = 5, graceMinutes = 2, exclude = NULL) {
  res <- liveReservations(path)
  res <- res[!res$id %in% exclude, , drop = FALSE]
  if (NROW(res)) {
    ageMinutes <- pmax(as.numeric(difftime(Sys.time(), res$created, units = "mins")) - graceMinutes, 0)
    share <- exp(-ageMinutes / loadWindowMinutes)
    byHost <- function(x) vapply(nodes$host, function(h) sum(x[res$host %in% h], na.rm = TRUE), numeric(1))
    reserved <- byHost(res$workers)
    subtract <- byHost(res$workers * share)
    reservedGB <- byHost(res$memGB)
    subtractGB <- byHost(res$memGB * share)
  } else {
    reserved <- subtract <- reservedGB <- subtractGB <- rep(0, NROW(nodes))
  }
  nodes$reserved <- as.integer(reserved)
  if (!is.null(nodes$mem_free_gb)) {
    nodes$mem_free_gb <- pmax(as.numeric(nodes$mem_free_gb) - subtractGB, 0)
    nodes$reserved_gb <- reservedGB
  }
  free <- as.numeric(nodes$free_est) - round(subtract, 3)
  ## every host keeps getOption("clusters.keepFreeCores", 2) cores for its other users, whatever its load
  if (!is.null(nodes$cores_total))
    free <- pmin(free, as.numeric(nodes$cores_total) - getOption("clusters.keepFreeCores", 2) - reserved)
  nodes$free_est <- pmax(free, 0)
  nodes
}

## Replace reservation `id`'s rows with `alloc` (`host`, `assign`), when a cluster's workers move. A host
## whose workers rose gets `created = now`, so builders count the new workers in full until the load
## average shows them (see freeCoresLessReserved()); the others keep their time. Memory is re-booked at
## `memPerWorkerGB` a worker, or, when not given, at what each host was booked at before.
.rebookCores <- function(id, alloc, memPerWorkerGB = NULL, path = getOption("clusters.reservationsPath")) {
  alloc <- alloc[alloc$assign > 0, , drop = FALSE]
  file <- reservationsPath(path)
  .withReservationLock(file, {
    res <- if (file.exists(file))
      .readReservations(file) else .emptyReservations()
    mine <- res[res$id %in% id, , drop = FALSE]
    pid <- if (NROW(mine)) mine$pid[1] else Sys.getpid()
    before <- mine$workers[match(alloc$host, mine$host)]
    created <- mine$created[match(alloc$host, mine$host)]
    rose <- is.na(before) | alloc$assign > before
    created[rose] <- Sys.time()
    res <- rbind(res[!res$id %in% id, , drop = FALSE],
                 data.frame(id = rep(id, NROW(alloc)), pid = rep(as.integer(pid), NROW(alloc)),
                            host = alloc$host, workers = as.integer(alloc$assign), created = created,
                            memGB = if (is.null(memPerWorkerGB)) alloc$assign * (mine$memGB / mine$workers)[match(alloc$host, mine$host)]
                                    else .bookedMemoryGB(alloc$assign, memPerWorkerGB),
                            stringsAsFactors = FALSE))
    saveRDS(res, file)
  })
  invisible(id)
}
