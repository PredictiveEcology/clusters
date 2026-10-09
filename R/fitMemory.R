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

## The recorded rows of fitMemory.rds within the day window (none: a zero-row data frame)
.fitMemoryRecent <- function(path = getOption("clusters.reservationsPath")) {
  none <- data.frame(runName = character(0), time = .POSIXct(numeric(0)), memMaxGB = numeric(0))
  file <- .fitMemoryFile(path)
  tryCatch(if (file.exists(file)) .hostSpeedRecent(.withReservationLock(file, readRDS(file))) else none,
           error = function(e) none)
}

## GB each worker is expected to need, in this order:
## 1. the largest worker memory (`memMaxGB`) of the most recent record for `runName`: the same fit, last time;
## 2. the largest `memMaxGB` of any runName in the window: fits actually run on this cluster, so cautious;
## 3. `getOption("clusters.workerMemoryGB", 14)`. Measured 2026-10-09 on 15 hosts, 457 FireSense DEoptim
##    workers' peak resident memory (VmHWM): median 4.5 GB, 90th percentile about 6.5 GB, maximum 14.1 GB.
## A fit's first recorded chunk replaces (3) with (1), and a running cluster re-reads it when it rebalances.
.memPerWorkerGB <- function(runName = NULL, path = getOption("clusters.reservationsPath")) {
  rows <- .fitMemoryRecent(path)
  mine <- rows[rows$runName %in% as.character(runName), , drop = FALSE]
  if (NROW(mine)) return(mine$memMaxGB[which.max(mine$time)])
  if (NROW(rows)) return(max(rows$memMaxGB))
  getOption("clusters.workerMemoryGB", 14)
}
