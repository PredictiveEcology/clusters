#' Book cores and memory for a job that is not a cluster
#'
#' @description
#' The reservation ledger (see `?reservations`) only knew the PSOCK workers a cluster build hands out, so a
#' long-running job in an ordinary R process -- a FireSense prediction is one SpaDES simulation of about
#' 38 GB and 1-2 cores, run as a tmux pane by `SpaDES.project::experimentTmux()` -- was invisible to it,
#' and a fit placed its workers on a host the predictions had already filled (2026-10-13).
#' `reserveCapacity()` books such a job in the same ledger, under the same caps as a build (`.fitCapacity()`:
#' every worker and other load within the host's threads less `options(clusters.keepFreeCores)`, and memory
#' used plus the job's within `1 - options(clusters.memoryHeadroom)` of the host's), so a build that follows
#' subtracts the job's cores and memory ([freeCoresLessReserved()]). It waits like a build waits, up to
#' `waitSeconds` (`options(clusters.waitForCores)`), until the whole of `cores` and `memGB` fits on one of
#' the `hosts`, probing every `interval` seconds, and then stops with the shortfall. Among several `hosts`
#' it takes the first, in the given order, where the job fits now. The probe, the decision and the booking
#' run under the lock every build allocates under (`.withAllocationLock()`).
#'
#' The booking is held until [releaseCapacity()] releases it or R exits (`reg.finalizer(onexit = TRUE)`),
#' whether or not the token is kept, and -- as for a cluster -- it is dropped when the owning process is
#' gone, since every read of the ledger drops the bookings of dead processes. [withCapacity()] books around
#' a piece of code and releases when it ends, however it ends.
#'
#' @param cores Cores the job needs; booked whole (`ceiling(cores)`), at least 1.
#' @param memGB Memory the job needs, in GB.
#' @param hosts The hosts the job may run on, in order of preference, named as the cluster builds name them
#'   in their `hosts`: the ledger is keyed by those names. The default, `"localhost"`, is this machine as
#'   every build running on it books it (`plan_psock_min()` renames the machine's own SSH alias to
#'   `"localhost"`).
#' @param probe `NULL`, or a function returning the node table for `hosts`, one row each with
#'   `cores_total`, `free_est`, `loadavg_5` (or the window of `load_memory`), `mem_available_gb` and
#'   `mem_total_gb`, as `.probeNodes()` builds it from a probe cluster. `NULL` measures this host in this
#'   process (`.probeHost()`), which is only possible when `hosts` is this host alone; a job choosing among
#'   other hosts gives a probe that can see them.
#' @param load_memory,fraction The load-average window and headroom of [parallelly::freeCores()], as in
#'   [plan_psock_min()].
#' @param waitSeconds How long to wait for the job to fit (`options(clusters.waitForCores)`, default 8 hours).
#' @param interval Seconds between probes.
#' @param sleep,now Clock functions, replaceable in tests.
#' @return `reserveCapacity()` an environment, the token: `id` (the reservation's), `host` (where the job
#'   was booked), `cores` and `memGB`. `releaseCapacity()` returns invisibly; `withCapacity()` the value of
#'   `expr`.
#' @examples
#' \dontrun{
#' ## a prediction job: 2 cores and 38 GB on this host, waiting up to 2 h for them
#' options(clusters.waitForCores = 2 * 3600)
#' token <- reserveCapacity(cores = 2, memGB = 38)
#' sim <- SpaDES.core::simInitAndSpades(...)
#' releaseCapacity(token)
#' ## or around the code, released however it ends
#' sim <- withCapacity(2, 38, SpaDES.core::simInitAndSpades(...))
#' }
#' @rdname reserveCapacity
#' @export
reserveCapacity <- function(cores, memGB, hosts = "localhost", probe = NULL,
                            load_memory = "5min", fraction = 0.9,
                            waitSeconds = .clustersOption("waitForCores"), interval = 60,
                            sleep = Sys.sleep, now = Sys.time) {
  stopifnot(is.numeric(cores), length(cores) == 1L, cores > 0, is.numeric(memGB), length(memGB) == 1L,
            memGB >= 0, is.character(hosts), length(hosts) >= 1L)
  cores <- as.integer(ceiling(cores))
  if (is.null(probe)) {
    thisHost <- .thisMachine()
    if (!all(hosts %in% thisHost))
      stop("reserveCapacity() can only measure this host (", thisHost[2], ") itself; a job choosing among ",
           paste(hosts, collapse = ", "), " needs a `probe` that can see them", call. = FALSE)
    probe <- function() .nodeTable(hosts[1], list(.probeHost(load_memory, fraction)))
  }
  ## the job's memory per core, so the build's own memory cap (so many workers of so much each) asks whether
  ## the whole of `memGB` fits
  memPerCore <- memGB / cores
  capacity <- function()
    .fitCapacity(probe(), total = cores, load_memory, memPerWorkerGB = memPerCore, capWorkers = FALSE)
  id <- .newReservationId("job")
  alloc <- .allocateWhenAvailable(capacity, total = cores, waitSeconds = waitSeconds, interval = interval,
                                  sleep = sleep, now = now, what = "cores",
                                  allocate = function(nodes) .jobAllocate(nodes, cores),
                                  book = function(alloc) reserveCores(alloc, id = id, memPerWorkerGB = memPerCore))
  token <- new.env(parent = emptyenv())
  token$id <- id
  token$host <- alloc$host[alloc$assign > 0][1]
  token$cores <- cores
  token$memGB <- memGB
  ## held here until released, so a token the caller did not keep is not released at the next garbage
  ## collection; the finalizer releases it when R exits
  assign(id, token, envir = .capacityTokens)
  reg.finalizer(token, function(e) try(releaseCores(id = id), silent = TRUE), onexit = TRUE)
  message("clusters: booked ", cores, " cores and ", memGB, " GB on ", token$host, " (", id, ")")
  token
}

## The tokens of this process's bookings, by reservation id, until released
.capacityTokens <- new.env(parent = emptyenv())

#' @param token What [reserveCapacity()] returned.
#' @rdname reserveCapacity
#' @export
releaseCapacity <- function(token) {
  stopifnot(is.environment(token), is.character(token$id))
  if (exists(token$id, envir = .capacityTokens, inherits = FALSE)) rm(list = token$id, envir = .capacityTokens)
  releaseCores(id = token$id)
}

#' @param expr Code to run with the booking held; released when it ends, however it ends.
#' @param ... Passed to [reserveCapacity()].
#' @rdname reserveCapacity
#' @export
withCapacity <- function(cores, memGB, expr, ...) {
  token <- reserveCapacity(cores, memGB, ...)
  on.exit(releaseCapacity(token), add = TRUE)
  expr
}

## Where a job of `cores` goes: all of them on the first host of `nodes` with that many free (as
## .fitCapacity() capped them), none anywhere when no host has. The data.frame .speedAllocate() returns,
## so .allocateWhenAvailable() and reserveCores() read it the same way.
.jobAllocate <- function(nodes, cores) {
  free <- floor(pmax(as.numeric(nodes$free_est), 0))
  assign <- integer(NROW(nodes))
  i <- which(free >= cores)[1]
  if (!is.na(i)) assign[i] <- cores
  out <- data.frame(host = nodes$host, cores_total = as.numeric(nodes$cores_total), free_est = free,
                    assign = assign, stringsAsFactors = FALSE)
  attr(out, "total_requested") <- cores
  attr(out, "total_assigned") <- sum(assign)
  out
}
