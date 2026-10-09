## How much memory does a worker need? Placement counted only cores (FireSense, 2026-10-09: host `core`,
## 187 GB, was filled to 187.1 GB by DEoptim workers and hung for 1.5 h, killing every fit with a worker
## there). Memory per worker differs ~3x between fits (ELFs): birds' 12 workers averaged 3.9 GB with a
## maximum of 13.1 GB, sbw's 11 averaged 3.3 GB, maximum 8.4 GB. DEoptimIterative() saves each computed
## chunk's per-worker peak memory beside the core-reservation ledger (fitMemory.rds, one row per chunk,
## like hostSpeed.rds but not per host), and a later build or refit of the same runName sizes its hosts
## from it (.fitCapacity()).

.fitMemoryFile <- function(path = getOption("clusters.reservationsPath"))
  file.path(dirname(reservationsPath(path)), "fitMemory.rds")

.fitMemoryColumns <- c("runName", "id", "time", "workers", "memMedianGB", "memMaxGB")

## GB in `field` (a line "Field:   123456 kB") of a /proc file; NA where it cannot be read
.procMemoryGB <- function(field, file) {
  lines <- tryCatch(readLines(file, warn = FALSE), error = function(e) character(0), warning = function(w) character(0))
  line <- grep(paste0("^", field, ":"), lines, value = TRUE)
  if (!length(line)) return(NA_real_)
  as.numeric(gsub("[^0-9]", "", line[1])) / 1024^2
}

## This process's peak resident memory (VmHWM), in GB. Read with every evaluation (see .memoObjFun()): a
## small file, negligible beside an evaluation that takes seconds.
.peakRssGB <- function() .procMemoryGB("VmHWM", "/proc/self/status")

## Append the memory of one chunk of evaluations (a list of `member$evaluations` data frames with `host`,
## `pid` and `peakGB`) to fitMemory.rds: one row per chunk, summarising its workers' peaks (each worker's
## largest reading). `id` names the chunk and replaces its earlier row (see .recordHostSpeed()). Never
## fails the fit.
.recordFitMemory <- function(evaluations, runName, id, path = getOption("clusters.reservationsPath")) {
  tryCatch({
    evaluations <- Filter(function(x) is.data.frame(x) && all(c("host", "pid", "peakGB") %in% names(x)),
                          evaluations)
    if (!length(evaluations)) return(invisible(NULL))
    ev <- do.call(rbind, lapply(evaluations, function(x) x[c("host", "pid", "peakGB")]))
    peak <- vapply(split(ev$peakGB, interaction(ev$host, ev$pid, drop = TRUE)),
                   function(p) if (all(is.na(p))) NA_real_ else max(p, na.rm = TRUE), numeric(1))
    peak <- peak[!is.na(peak)]
    if (!length(peak)) return(invisible(NULL))
    new <- data.frame(runName = as.character(runName), id = id, time = Sys.time(), workers = length(peak),
                      memMedianGB = stats::median(peak), memMaxGB = max(peak), stringsAsFactors = FALSE)
    .replaceRecords(.fitMemoryFile(path), new, .fitMemoryColumns)
    invisible(new)
  }, error = function(e) {
    warning("Could not record fit memory: ", conditionMessage(e), call. = FALSE)
    invisible(NULL)
  })
}

## The largest worker memory (GB) of the most recent record for `runName`, or NA.
.fitMemoryLast <- function(runName, path = getOption("clusters.reservationsPath")) {
  if (is.null(runName)) return(NA_real_)
  tryCatch({
    file <- .fitMemoryFile(path)
    if (!file.exists(file)) return(NA_real_)
    rows <- .withReservationLock(file, readRDS(file))
    rows <- .hostSpeedRecent(rows[rows$runName %in% as.character(runName), , drop = FALSE])
    if (NROW(rows)) rows$memMaxGB[which.max(rows$time)] else NA_real_
  }, error = function(e) NA_real_)
}

## GB each worker is expected to need: what the fit used last time (`runName`'s latest record), else
## `estimateGB`, else NA (no memory cap).
.memPerWorkerGB <- function(runName, estimateGB = NA_real_, path = getOption("clusters.reservationsPath")) {
  last <- .fitMemoryLast(runName, path)
  if (is.na(last)) as.numeric(estimateGB)[1] else last
}

## Memory per worker before any fit has recorded one: the size of the objects shipped to the workers
## (`objsNeeded` in `envir`) times `getOption("clusters.workerMemoryFactor", 3)`, and at least 1 GB. Measured
## FireSense workers held several GB beyond the objects shipped to them (R, packages, the objective's
## working copies, rasters read back from disk), so the factor is a deliberate over-estimate, which the
## first recorded chunk of the fit replaces. `object.size()` does not see memory held outside R (terra), so
## it is a floor for those objects. NA when there are no objects.
.estimateWorkerMemoryGB <- function(objsNeeded, envir, factor = getOption("clusters.workerMemoryFactor", 3)) {
  if (is.null(objsNeeded) || !length(objsNeeded)) return(NA_real_)
  bytes <- sum(vapply(mget(unlist(objsNeeded), envir = envir), function(o) as.numeric(utils::object.size(o)),
                      numeric(1)))
  max(1, factor * bytes / 1024^3)
}
