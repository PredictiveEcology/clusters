## One implementation of the /proc/meminfo read. It used to exist three times --
## once as ramUsageGB() and twice inlined into monitorCluster()'s clusterEvalQ
## calls, because a worker that lacks this package cannot call
## `clusters::ramUsageGB()`. Shipping the function itself (see .bare) keeps that
## property without the copies.
#' @keywords internal
.ramFromMeminfo <- function(path = "/proc/meminfo") {
  na <- c(used = NA_real_, total = NA_real_)
  if (!file.exists(path)) return(na)
  lines <- readLines(path, warn = FALSE)
  kb <- function(key) {
    ln <- grep(paste0("^", key, ":[[:space:]]"), lines, value = TRUE)
    if (!length(ln)) return(NA_real_)
    as.numeric(sub(paste0("^", key, ":[[:space:]]+([0-9]+).*"), "\\1", ln[[1L]]))
  }
  total <- kb("MemTotal")
  avail <- kb("MemAvailable")
  if (is.na(total) || is.na(avail)) return(na)
  c(used = round((total - avail) / 1048576, 1), total = round(total / 1048576, 1))
}

## Re-home a function in the global environment so it serialises to a worker
## standalone, carrying no reference to this package's namespace. That is what
## lets a host report memory and threads without `clusters` installed on it.
#' @keywords internal
.bare <- function(f) {
  environment(f) <- globalenv()
  f
}

## Ask every worker for one numeric, in worker order, never shrinking or
## reordering the result. clusterCall returns one element per worker, and the
## caller matches those to `cores` by position, so a worker that errors must
## still occupy its slot.
#' @keywords internal
.probeWorkers <- function(cl, fun, pick) {
  probe <- .bare(function(g, pick) tryCatch(as.numeric(g()[[pick]]), error = function(e) NA_real_))
  out <- parallel::clusterCall(cl, probe, .bare(fun), pick)
  vapply(out, function(x) if (length(x) == 1L) as.numeric(x) else NA_real_, numeric(1))
}

#' Format one host's memory as `used/totalGB`
#' @keywords internal
.fmtRam <- function(used, total) {
  if (is.na(used) || is.na(total)) return("?/?GB")
  sprintf("%.1f/%.1fGB", used, total)
}

## Column i is as wide as the wider of the host's name and the widest memory
## string it can produce (used == total).
#' @keywords internal
.monitorWidths <- function(cores, totalRam) {
  widest <- vapply(totalRam, function(t) nchar(.fmtRam(t, t)), integer(1))
  pmax(nchar(cores), widest)
}

#' Render one right-aligned row of the monitor display
#' @keywords internal
.monitorRow <- function(vals, widths, pad = 2L) {
  vals[is.na(vals)] <- ""
  paste(mapply(function(v, w) sprintf("%*s%s", w, v, strrep(" ", pad)), vals, widths),
        collapse = "")
}

#' Get RAM usage in GB on the current machine
#'
#' Reads \file{/proc/meminfo} and returns used and total memory in gigabytes.
#' Used is `MemTotal - MemAvailable`, that is, everything the kernel cannot hand
#' out immediately, so page cache in active use counts as used.
#'
#' @return A list with `used_gb` and `total_gb`, both `NA_real_` where
#'   \file{/proc/meminfo} is absent (any non-Linux host) or unreadable.
#' @export
#' @examples
#' ramUsageGB()
ramUsageGB <- function() {
  x <- .ramFromMeminfo()
  list(used_gb = unname(x[["used"]]), total_gb = unname(x[["total"]]))
}

#' Watch active threads and memory across a cluster's hosts
#'
#' @description
#' Polls every worker for its active-thread count and its memory use, and
#' redraws two aligned rows in place: threads on the first, `used/totalGB` on
#' the second. Interrupt it (Ctrl-C) to stop; it then prints the per-host peaks
#' it saw and returns them.
#'
#' Neither probe needs `clusters` installed on the hosts. The functions are
#' re-homed in the global environment before being sent, so they travel whole
#' rather than as a reference to this package's namespace.
#'
#' @param cl A running cluster with one worker per host, in the same order as
#'   `cores`. If missing, one is built over SSH from `cores` and stopped on exit.
#' @param cores Character vector of host names, in the cluster's worker order.
#' @param pad Integer; spaces between columns.
#' @param interval Numeric; seconds between polls.
#'
#' @return Invisibly, a list with `threads` (peak active threads per host) and
#'   `ram` (peak memory used per host, GB). Returned on interrupt.
#'
#' @note The display uses ANSI erase sequences to redraw in place. Redirect it
#'   to a file and you get one block per tick instead of a live display.
#' @export
#' @examples
#' \dontrun{
#' hosts <- c("birds", "biomass", "camas")
#' peaks <- monitorCluster(cores = hosts)   # Ctrl-C to stop
#' peaks$ram
#' }
monitorCluster <- function(cl, cores, pad = 2, interval = 1) {
  stopifnot(length(cores) > 0)

  if (missing(cl)) {
    cl <- parallelly::makeClusterPSOCK(workers = cores, rshcmd = "ssh", homogeneous = FALSE)
    on.exit(try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)
  }
  if (length(cl) != length(cores))
    stop("`cl` has ", length(cl), " workers but `cores` names ", length(cores),
         " hosts. Results are matched by position, so they must correspond.",
         call. = FALSE)

  parallel::clusterCall(cl, .bare(function(p) .libPaths(p)), .libPaths())

  ## Total memory is fixed for the life of the cluster; ask once.
  totalRam <- .probeWorkers(cl, .ramFromMeminfo, "total")
  names(totalRam) <- cores
  widths <- .monitorWidths(cores, totalRam)
  header <- .monitorRow(cores, widths, pad)

  peakThreads <- stats::setNames(rep(0, length(cores)), cores)
  peakRam <- stats::setNames(rep(NA_real_, length(cores)), cores)
  firstTick <- TRUE

  cat(header, "\n", sep = "")
  tryCatch({
    repeat {
      threads <- .probeWorkers(cl, numActiveThreads, 1L)
      usedRam <- .probeWorkers(cl, .ramFromMeminfo, "used")

      peakThreads <- pmax(peakThreads, ifelse(is.na(threads), 0, threads))
      peakRam <- pmax(peakRam, usedRam, na.rm = TRUE)

      threadStrs <- ifelse(is.na(threads), NA_character_, as.character(threads))
      ramStrs <- mapply(.fmtRam, usedRam, totalRam)

      if (firstTick) {
        cat(.monitorRow(threadStrs, widths, pad), "\n", sep = "")
        cat(.monitorRow(ramStrs, widths, pad), sep = "")
        firstTick <- FALSE
      } else {
        cat("\033[A\033[2K\r", .monitorRow(threadStrs, widths, pad), "\n", sep = "")
        cat("\033[2K\r", .monitorRow(ramStrs, widths, pad), sep = "")
      }
      utils::flush.console()
      Sys.sleep(interval)
    }
  }, interrupt = function(e) {
    cat("\033[2K\r\n")
    cat("Peaks seen:\n")
    cat(header, "\n", sep = "")
    cat(.monitorRow(as.character(peakThreads), widths, pad), "\n", sep = "")
    cat(.monitorRow(vapply(seq_along(cores), function(i) .fmtRam(peakRam[[i]], totalRam[[i]]),
                           character(1)), widths, pad), "\n", sep = "")
    cat(sprintf("Peak threads across all hosts: %d\n", as.integer(sum(peakThreads))))
    invisible(list(threads = peakThreads, ram = peakRam))
  })
}
