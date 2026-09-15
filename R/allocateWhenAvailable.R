## Wait for the whole population a cluster build asked for.
##
## A DEoptim fit's population is sized to its problem (about 10 x the number of parameters)
## and to its cluster, one worker per member. Starting it with a fraction of the workers it
## asked for gives a fit that runs for days on a handful of cores (FireSense phase 2,
## 2026-09-15: 5, 2 and 7 of 100 workers). Waiting instead makes the number of concurrent
## fits limit itself to what the hosts can hold, without the user having to know it.

#' Allocate workers once the whole requested population is free
#'
#' Probes the hosts, allocates with [.ht_allocate_min()] (real cores before hyperthreads),
#' and returns as soon as at least `ceiling(minFraction * total)` workers can be assigned.
#' Otherwise it waits `interval` seconds and probes again, until `waitSeconds` have passed,
#' and then stops with the shortfall rather than starting with fewer workers. A request
#' larger than every core on the hosts stops at once, since waiting cannot satisfy it.
#'
#' @param probe A function returning the node table (`host`, `cores_total`, `free_est`),
#'   with reservations already subtracted.
#' @param total Workers requested.
#' @param beta Weight of hyperthreads in [.ht_allocate_min()].
#' @param minFraction Smallest fraction of `total` to start with; 1 (the default, from
#'   `options(clusters.minWorkersFraction)`) means the whole population.
#' @param waitSeconds How long to keep waiting (`options(clusters.waitForCores)`).
#' @param interval Seconds between probes.
#' @param sleep,now Clock functions, replaceable in tests.
#' @return The allocation data.frame from [.ht_allocate_min()].
#' @keywords internal
.allocateWhenAvailable <- function(probe, total, beta = 0.5,
                                   minFraction = getOption("clusters.minWorkersFraction", 1),
                                   waitSeconds = getOption("clusters.waitForCores", 0),
                                   interval = 60, sleep = Sys.sleep, now = Sys.time) {
  stopifnot(is.function(probe), is.numeric(minFraction), length(minFraction) == 1L,
            minFraction > 0, minFraction <= 1)
  needed <- as.integer(ceiling(minFraction * total))
  started <- now()
  deadline <- started + waitSeconds
  repeat {
    nodes <- probe()
    alloc <- .ht_allocate_min(nodes, total = total, beta = beta)
    got <- as.integer(sum(alloc$assign))
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
