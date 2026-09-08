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
#' Each successful build records `host -> workers` under a reservation id owned
#' by the building process; [freeCoresLessReserved()] subtracts those from the
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
#'   with columns `id`, `pid`, `host`, `workers`, `created`; `reserveCores()`
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

.pidAlive <- function(pid) {
  vapply(pid, function(p) {
    if (is.na(p)) return(FALSE)
    if (.Platform$OS.type == "unix") dir.exists(file.path("/proc", p))
    else !inherits(try(system2("tasklist", c("/FI", shQuote(paste0("PID eq ", p))),
                               stdout = TRUE, stderr = FALSE), silent = TRUE), "try-error")
  }, logical(1))
}

.emptyReservations <- function()
  data.frame(id = character(0), pid = integer(0), host = character(0),
             workers = integer(0), created = as.POSIXct(character(0)),
             stringsAsFactors = FALSE)

#' @rdname reservations
#' @export
liveReservations <- function(path = getOption("clusters.reservationsPath")) {
  file <- reservationsPath(path)
  if (!file.exists(file)) return(.emptyReservations())
  .withReservationLock(file, {
    res <- tryCatch(readRDS(file), error = function(e) .emptyReservations())
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
#' @rdname reservations
#' @export
reserveCores <- function(alloc, id = basename(tempfile("resv")), pid = Sys.getpid(),
                         path = getOption("clusters.reservationsPath")) {
  stopifnot(all(c("host", "assign") %in% names(alloc)))
  alloc <- alloc[alloc$assign > 0, , drop = FALSE]
  if (!NROW(alloc)) return(invisible(id))
  file <- reservationsPath(path)
  .withReservationLock(file, {
    res <- if (file.exists(file))
      tryCatch(readRDS(file), error = function(e) .emptyReservations()) else .emptyReservations()
    res <- res[.pidAlive(res$pid), , drop = FALSE]
    res <- rbind(res, data.frame(id = id, pid = as.integer(pid), host = alloc$host,
                                 workers = as.integer(alloc$assign),
                                 created = Sys.time(), stringsAsFactors = FALSE))
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
    res <- tryCatch(readRDS(file), error = function(e) .emptyReservations())
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
#' @return `nodes` with `free_est` reduced by every live reservation on that
#'   host, floored at zero, plus a `reserved` column for reporting. Reservations
#'   held by this process count too: a master that already built one cluster is
#'   genuinely using those cores while it builds the next.
#' @export
freeCoresLessReserved <- function(nodes,
                                  path = getOption("clusters.reservationsPath")) {
  res <- liveReservations(path)
  reserved <- if (NROW(res))
    vapply(nodes$host, function(h) sum(res$workers[res$host %in% h]), numeric(1))
  else rep(0, NROW(nodes))
  nodes$reserved <- as.integer(reserved)
  nodes$free_est <- pmax(as.numeric(nodes$free_est) - reserved, 0)
  nodes
}
