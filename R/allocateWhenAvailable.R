## Wait for the whole population a cluster build asked for.
##
## A DEoptim fit's population is sized to its problem (about 10 x the number of parameters)
## and to its cluster, one worker per member. Starting it with a fraction of the workers it
## asked for gives a fit that runs for days on a handful of cores (FireSense phase 2,
## 2026-09-15: 5, 2 and 7 of 100 workers). Waiting instead makes the number of concurrent
## fits limit itself to what the hosts can hold, without the user having to know it.

#' Allocate workers once the whole requested population is free
#'
#' Probes the hosts, allocates with [.speedAllocate()] (where workers run fastest),
#' and returns as soon as at least `ceiling(minFraction * total)` workers can be assigned.
#' Otherwise it waits `interval` seconds and probes again, until `waitSeconds` have passed,
#' and then stops with the shortfall rather than starting with fewer workers. A request
#' larger than every core on the hosts stops at once, since waiting cannot satisfy it.
#'
#' @param probe A function returning the node table (`host`, `cores_total`, `free_est`),
#'   with reservations already subtracted. A `ram_lost` column (see `.fitCapacity()`) is the cores each
#'   host's free memory cannot hold workers for: the workers lost to it are neither waited for nor an error, so
#'   the cluster is built smaller.
#' @param total Workers requested.
#' @param beta What a hyperthread adds to a core, in [.speedAllocate()].
#' @param minFraction Smallest fraction of `total` to start with; 1 (the default, from
#'   `options(clusters.minWorkersFraction)`) means the whole population.
#' @param waitSeconds How long to keep waiting (`options(clusters.waitForCores)`).
#' @param interval Seconds between probes.
#' @param sleep,now Clock functions, replaceable in tests.
#' @param book `NULL`, or a function of the allocation that books it (see [reserveCores()]). Each probe,
#'   allocation and booking runs under one lock shared with every other build and rebalance
#'   (`.withAllocationLock()`), so no other cluster decides between this probe and this booking.
#' @return The allocation data.frame from [.speedAllocate()].
#' @keywords internal
.allocateWhenAvailable <- function(probe, total, beta = 0.75,
                                   minFraction = getOption("clusters.minWorkersFraction", 1),
                                   waitSeconds = getOption("clusters.waitForCores", 0),
                                   interval = 60, sleep = Sys.sleep, now = Sys.time, book = NULL) {
  stopifnot(is.function(probe), is.numeric(minFraction), length(minFraction) == 1L,
            minFraction > 0, minFraction <= 1)
  requested <- as.integer(ceiling(minFraction * total))
  started <- now()
  deadline <- started + waitSeconds
  attempt <- function() {
    nodes <- probe()
    alloc <- .speedAllocate(nodes, total = total, beta = beta)
    got <- as.integer(sum(alloc$assign))
    needed <- .neededAfterMemory(requested, nodes)
    if (got >= needed && is.function(book)) book(alloc)
    list(nodes = nodes, alloc = alloc, got = got, needed = needed)
  }
  repeat {
    a <- if (is.function(book)) .withAllocationLock(attempt()) else attempt()
    nodes <- a$nodes; alloc <- a$alloc; got <- a$got; needed <- a$needed
    if (got >= needed) return(alloc)

    freeByHost <- paste0(nodes$host, "=", nodes$free_est, "/", nodes$cores_total, collapse = ", ")
    if (sum(nodes$cores_total) < needed)
      stop("This cluster needs ", needed, " workers, more workers than the ",
           sum(nodes$cores_total), " cores on its hosts (free/total: ", freeByHost,
           "), so it could never start. Ask for fewer workers or add hosts.", call. = FALSE)

    waited <- round(as.numeric(difftime(now(), started, units = "mins")), 1)
    if (now() >= deadline)
      stop("could only get ", got, " of ", needed, " workers after waiting ", waited,
           " min (options(clusters.waitForCores)); free/total cores by host: ", freeByHost,
           ". Not starting with fewer workers; options(clusters.minWorkersFraction = ) allows a ",
           "partial start.", call. = FALSE)

    message("Waiting for cores: could get ", got, " of ", needed, " workers (free/total cores by host: ",
            freeByHost, "); probing again in ", interval, " s, until ",
            format(deadline, "%Y-%m-%d %H:%M"), " at most")
    sleep(interval)
  }
}

## Workers lost to memory (`nodes$ram_lost`) are not waited for: the build is smaller by them, but still
## has one worker at least, and a shortage of cores as well as memory still waits.
.neededAfterMemory <- function(needed, nodes) {
  lost <- if (is.null(nodes$ram_lost)) 0 else sum(nodes$ram_lost, na.rm = TRUE)
  as.integer(max(min(needed, 1L), needed - lost))
}
