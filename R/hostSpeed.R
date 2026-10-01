## Which hosts are slow? A DEoptim generation lasts as long as its slowest evaluation, and NP is the
## number of workers, so one consistently slow host slows every generation. DEoptimIterative() saves
## each fit's per-host speeds beside the core-reservation ledger; the next cluster build reads them
## and leaves out hosts that are slow, when the other hosts can still hold the whole population.

.hostSpeedFile <- function(path = getOption("clusters.reservationsPath"))
  file.path(dirname(reservationsPath(path)), "hostSpeed.rds")

.hostSpeedColumns <- c("host", "n", "median", "p90", "ratio", "time", "id")

## `speeds` rows newer than the window (`days` is getOption("clusters.hostSpeedDays", 30))
.hostSpeedRecent <- function(speeds, days = getOption("clusters.hostSpeedDays", 30), now = Sys.time())
  speeds[as.numeric(difftime(now, speeds$time, units = "days")) <= days, , drop = FALSE]

## Summarise the evaluations computed in one fit, show the table, and append it to hostSpeed.rds.
## `evaluations` is a list of `member$evaluations` data frames. Never fails the fit.
.recordHostSpeed <- function(evaluations, id, path = getOption("clusters.reservationsPath")) {
  tryCatch({
    evaluations <- Filter(function(x) is.data.frame(x) && "host" %in% names(x), evaluations)
    if (!length(evaluations)) return(invisible(NULL))
    tab <- workerSpeed(evaluations, by = "host")
    message("Evaluation speed by host (ratio: host median over the median of all evaluations):")
    reproducible::messageDF(tab)
    file <- .hostSpeedFile(path)
    new <- cbind(tab[c("host", "n", "median", "p90", "ratio")], time = Sys.time(), id = id,
                 stringsAsFactors = FALSE)
    .withReservationLock(file, {
      old <- if (file.exists(file)) tryCatch(readRDS(file), error = function(e) NULL)
      saveRDS(.hostSpeedRecent(rbind(old[.hostSpeedColumns], new)), file)
    })
    invisible(new)
  }, error = function(e) {
    warning("Could not record host speeds: ", conditionMessage(e), call. = FALSE)
    invisible(NULL)
  })
}

## The recorded speeds, or NULL when there is no file
.readHostSpeeds <- function(path = getOption("clusters.reservationsPath")) {
  file <- .hostSpeedFile(path)
  if (file.exists(file)) tryCatch(readRDS(file), error = function(e) NULL)
}

#' Leave slow hosts out of a cluster, if the others can hold it
#'
#' A host's recorded ratios (see [workerSpeed()]) are averaged, weighted by `n`, over the rows newer than
#' `getOption("clusters.hostSpeedDays", 30)` days. A host whose ratio is above `maxRatio` gets
#' `free_est = 0`, slowest first, and only while the remaining hosts' free cores still cover `total`.
#' Hosts with no record are kept.
#'
#' @param nodes data.frame with `host`, `nodename` (as recorded by the evaluations) and `free_est`.
#' @param speeds The data frame saved in `hostSpeed.rds` (`host`, `n`, `ratio`, `time`), or `NULL`.
#' @param total Workers requested.
#' @param maxRatio Ratio above which a host is slow; `getOption("clusters.slowHostRatio", 1.25)`.
#' @param days Age window in days of the records used.
#' @return `nodes`, with `free_est` set to 0 for the excluded hosts.
#' @keywords internal
.excludeSlowHosts <- function(nodes, speeds, total,
                              maxRatio = getOption("clusters.slowHostRatio", 1.25),
                              days = getOption("clusters.hostSpeedDays", 30)) {
  if (!NROW(speeds)) {
    message("No host speed records: no host left out.")
    return(nodes)
  }
  speeds <- .hostSpeedRecent(speeds, days)
  ratio <- vapply(nodes$nodename, function(h) {
    s <- speeds[speeds$host %in% h, , drop = FALSE]
    if (NROW(s)) stats::weighted.mean(s$ratio, s$n) else NA_real_
  }, numeric(1))
  free <- pmax(as.numeric(nodes$free_est), 0)
  excluded <- integer(0)
  for (i in order(-ratio)) {
    if (is.na(ratio[i]) || ratio[i] <= maxRatio) break
    if (sum(free[-c(excluded, i)]) < total) break
    excluded <- c(excluded, i)
  }
  if (length(excluded)) {
    nodes$free_est[excluded] <- 0
    message("Hosts left out as slow (ratio > ", maxRatio, "): ",
            paste0(nodes$host[excluded], "=", round(ratio[excluded], 2), collapse = ", "))
  } else {
    message("No host left out for speed (ratio > ", maxRatio, ").")
  }
  nodes
}
