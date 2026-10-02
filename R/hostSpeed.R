## Which hosts are slow? A DEoptim generation lasts as long as its slowest evaluation, and NP is the
## number of workers, so one consistently slow host slows every generation. DEoptimIterative() saves
## each fit's per-host speeds beside the core-reservation ledger; the next cluster build reads them
## and leaves out hosts that are slow, when the other hosts can still hold the whole population.

.hostSpeedFile <- function(path = getOption("clusters.reservationsPath"))
  file.path(dirname(reservationsPath(path)), "hostSpeed.rds")

.hostSpeedColumns <- c("host", "n", "median", "p90", "ratio", "time", "id", "workers")

## A recorded row is a full load when its `workers` (distinct worker pids on the host in that chunk) is at
## least this fraction of the host's largest recorded `workers`; getOption("clusters.hostSpeedFullLoad").
## Slowness comes from memory bandwidth, which only a loaded host runs short of, so a lightly used host's
## fast rows must not clear its slow flag.
.hostSpeedFullLoad <- function() getOption("clusters.hostSpeedFullLoad", 0.5)

## The rows of `speeds` that say how fast `host` is: recent, with a finite positive median and ratio, and
## a full load. Rows without `workers` (older records) are a full load.
.usableSpeeds <- function(speeds, host) {
  s <- .hostSpeedRecent(speeds[speeds$host %in% host, , drop = FALSE])
  s <- s[is.finite(s$ratio) & s$ratio > 0 & is.finite(s$median) & s$median > 0, , drop = FALSE]
  if (!NROW(s) || !"workers" %in% names(s) || all(is.na(s$workers))) return(s)
  s[is.na(s$workers) | s$workers >= .hostSpeedFullLoad() * max(s$workers, na.rm = TRUE), , drop = FALSE]
}

## `speeds` rows newer than the window (`days` is getOption("clusters.hostSpeedDays", 30))
.hostSpeedRecent <- function(speeds, days = getOption("clusters.hostSpeedDays", 30), now = Sys.time())
  speeds[as.numeric(difftime(now, speeds$time, units = "days")) <= days, , drop = FALSE]

## The evaluation records that say which host ran them
.withHost <- function(evaluations)
  Filter(function(x) is.data.frame(x) && "host" %in% names(x), evaluations)

## Append the per-host speeds of one chunk of evaluations to hostSpeed.rds. Called after every computed
## chunk, so a fit that is killed still leaves its records. `id` names the chunk (runName and chunk
## number): a chunk computed again, e.g. after a restart, replaces its earlier rows instead of adding a
## duplicate. `evaluations` is a list of
## `member$evaluations` data frames. Never fails the fit.
.recordHostSpeed <- function(evaluations, id, path = getOption("clusters.reservationsPath")) {
  tryCatch({
    evaluations <- .withHost(evaluations)
    if (!length(evaluations)) return(invisible(NULL))
    tab <- workerSpeed(evaluations, by = "host")
    perWorker <- workerSpeed(evaluations, by = "worker")
    tab$workers <- as.integer(table(perWorker$host)[tab$host])
    file <- .hostSpeedFile(path)
    new <- cbind(tab[setdiff(.hostSpeedColumns, c("time", "id"))], time = Sys.time(), id = id,
                 stringsAsFactors = FALSE)
    .withReservationLock(file, {
      old <- if (file.exists(file)) tryCatch(readRDS(file), error = function(e) NULL)
      old <- old[!old$id %in% id, .hostSpeedColumns, drop = FALSE]
      saveRDS(.hostSpeedRecent(rbind(old, new)), file)
    })
    invisible(new)
  }, error = function(e) {
    warning("Could not record host speeds: ", conditionMessage(e), call. = FALSE)
    invisible(NULL)
  })
}

## Show the per-host speeds of all the evaluations a fit computed
.showHostSpeed <- function(evaluations) {
  evaluations <- .withHost(evaluations)
  if (!length(evaluations)) return(invisible(NULL))
  message("Evaluation speed by host (ratio: host median over the median of all evaluations):")
  reproducible::messageDF(workerSpeed(evaluations, by = "host"))
}

## The recorded speeds, or NULL when there is no file
.readHostSpeeds <- function(path = getOption("clusters.reservationsPath")) {
  file <- .hostSpeedFile(path)
  if (file.exists(file)) tryCatch(readRDS(file), error = function(e) NULL)
}

#' Use slow hosts last, and only for the shortfall
#'
#' A host's recorded ratios (see [workerSpeed()]) are averaged, weighted by `n`, over the usable rows: those
#' newer than `getOption("clusters.hostSpeedDays", 30)` days, with a finite positive median and ratio, and
#' from a full load (`workers` at least `getOption("clusters.hostSpeedFullLoad", 0.5)` of the host's largest
#' recorded `workers`; rows without `workers` are a full load). Hosts whose ratio is above `maxRatio` are
#' slow; hosts with no usable row are kept as fast. If the fast hosts' free cores cover `total`, every slow
#' host gets `free_est = 0`. Otherwise slow hosts are added back, least slow first, each with `free_est` of
#' its free cores or the remaining shortfall, whichever is smaller. The allocator then gives `total`
#' workers, and `total` is the sum of the `free_est`, so the fast hosts are filled and the slow hosts carry
#' only the shortfall.
#'
#' @param nodes data.frame with `host`, `nodename` (as recorded by the evaluations) and `free_est`.
#' @param speeds The data frame saved in `hostSpeed.rds` (`host`, `n`, `median`, `ratio`, `time` and
#'   optionally `workers`), or `NULL`.
#' @param total Workers requested.
#' @param maxRatio Ratio above which a host is slow; `getOption("clusters.slowHostRatio", 1.25)`.
#' @return `nodes`, with `free_est` set to 0 for the hosts left out and lowered for the hosts capped.
#' @keywords internal
.excludeSlowHosts <- function(nodes, speeds, total,
                              maxRatio = getOption("clusters.slowHostRatio", 1.25)) {
  if (!NROW(speeds)) {
    message("No host speed records: no host left out.")
    return(nodes)
  }
  ratio <- vapply(nodes$nodename, function(h) {
    s <- .usableSpeeds(speeds, h)
    if (NROW(s)) stats::weighted.mean(s$ratio, s$n) else NA_real_
  }, numeric(1))
  free <- pmax(as.numeric(nodes$free_est), 0)
  slow <- which(!is.na(ratio) & ratio > maxRatio)
  slow <- slow[order(ratio[slow])]
  shortfall <- total - sum(free[setdiff(seq_along(free), slow)])
  allowed <- numeric(length(slow))
  for (k in seq_along(slow)) {
    if (shortfall <= 0) break
    allowed[k] <- min(free[slow[k]], shortfall)
    shortfall <- shortfall - allowed[k]
  }
  nodes$free_est[slow] <- allowed
  left <- allowed == 0
  capped <- allowed > 0 & allowed < free[slow]
  if (any(left))
    message("Hosts left out as slow (ratio > ", maxRatio, "): ",
            paste0(nodes$host[slow[left]], "=", round(ratio[slow[left]], 2), collapse = ", "))
  if (any(capped))
    message("Slow hosts (ratio > ", maxRatio, ") capped to the shortfall: ",
            paste0(nodes$host[slow[capped]], "=", round(ratio[slow[capped]], 2), " capped to ",
                   allowed[capped], " workers", collapse = ", "))
  if (!any(left | capped)) message("No host left out for speed (ratio > ", maxRatio, ").")
  nodes
}
