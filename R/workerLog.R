## The worker log file of each of `hosts` (one probe worker each in `cl`, in that order), named by host:
## `logPath` where the host can write its folder, creating it if need be; else a file in the host's own
## user cache folder. Every worker opens its log before it connects back and exits when it cannot, so a
## folder on a disk that only the master has (FireSense 2026-10-07: /mnt/fast) stopped every worker on
## the other hosts. NULL when `logPath` is NULL.
.hostLogPaths <- function(cl, hosts, logPath) {
  if (is.null(logPath)) return(NULL)
  ## global environment: sending the namespace function would load clusters on the worker
  workerLogPath <- .workerLogPath
  environment(workerLogPath) <- globalenv()
  logs <- unlist(parallel::clusterCall(cl, workerLogPath, logPath))
  names(logs) <- hosts
  moved <- logs != logPath
  if (any(moved))
    message("clusters: ", dirname(logPath), " cannot be written on ", paste(hosts[moved], collapse = ", "),
            "; worker logs there: ", paste(unique(logs[moved]), collapse = ", "))
  logs
}

## On a worker: `logPath`, or the file in this host's user cache folder that replaces it
.workerLogPath <- function(logPath) {
  dir.create(dirname(logPath), recursive = TRUE, showWarnings = FALSE)
  if (file.access(dirname(logPath), 2L) == 0L) return(logPath)
  own <- file.path(tools::R_user_dir("clusters", "cache"), "logs")
  dir.create(own, recursive = TRUE, showWarnings = FALSE)
  file.path(own, paste0(Sys.info()[["nodename"]], "_", basename(logPath)))
}

## The `outfile` for a worker on `host`: one per host (named), or the one for all
.hostOutfile <- function(outfile, host) {
  if (is.null(names(outfile))) outfile else unname(outfile[host])
}
