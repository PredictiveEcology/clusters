## How fast is each host? DEoptimIterative() saves each fit's per-host evaluation speeds beside the
## core-reservation ledger (hostSpeed.rds), with the workers the fit had on each host, to calibrate
## .workerSpeed() against.

.hostSpeedFile <- function(path = getOption("clusters.reservationsPath"))
  file.path(dirname(reservationsPath(path)), "hostSpeed.rds")

.hostSpeedColumns <- c("host", "n", "median", "p90", "ratio", "time", "id", "workers")

## `speeds` rows newer than the window (`days` is getOption("clusters.hostSpeedDays", 30))
.hostSpeedRecent <- function(speeds, days = getOption("clusters.hostSpeedDays", 30), now = Sys.time())
  speeds[as.numeric(difftime(now, speeds$time, units = "days")) <= days, , drop = FALSE]

## The evaluation records that say which host ran them
.withHost <- function(evaluations)
  Filter(function(x) is.data.frame(x) && "host" %in% names(x), evaluations)

## Add `new` rows to the records in `file` (hostSpeed.rds, fitMemory.rds), dropping the rows of the same
## `id` and those older than the window. One lock for the read and the write.
.replaceRecords <- function(file, new, columns) {
  .withReservationLock(file, {
    old <- if (file.exists(file)) tryCatch(readRDS(file), error = function(e) NULL)
    old <- old[!old$id %in% new$id, columns, drop = FALSE]
    saveRDS(.hostSpeedRecent(rbind(old, new)), file)
  })
}

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
    .replaceRecords(file, new, .hostSpeedColumns)
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
