#' Get RAM usage in GB on the current machine
#'
#' Reads \file{/proc/meminfo} to obtain total and available RAM and returns
#' used and total amounts in gigabytes. Used RAM is computed as
#' \code{MemTotal - MemAvailable} (i.e., everything not immediately reclaimable
#' by the kernel, including buffers and page cache that are in active use).
#'
#' @return A named list with two numeric elements:
#'   \describe{
#'     \item{\code{used_gb}}{RAM in use (GB, one decimal place).}
#'     \item{\code{total_gb}}{Total installed RAM (GB, one decimal place).}
#'   }
#'   Both elements are \code{NA_real_} on non-Linux systems where
#'   \file{/proc/meminfo} is absent.
#'
#' @export
ramUsageGB <- function() {
  if (!file.exists("/proc/meminfo"))
    return(list(used_gb = NA_real_, total_gb = NA_real_))
  lines <- readLines("/proc/meminfo", warn = FALSE)
  get_kb <- function(key) {
    ln <- grep(paste0("^", key, ":\\s"), lines, value = TRUE)
    if (!length(ln)) return(NA_real_)
    as.numeric(sub(paste0("^", key, ":\\s+(\\d+).*"), "\\1", ln[[1L]]))
  }
  total_kb <- get_kb("MemTotal")
  avail_kb <- get_kb("MemAvailable")
  list(
    used_gb  = round((total_kb - avail_kb) / 1048576, 1),
    total_gb = round(total_kb              / 1048576, 1)
  )
}

#' Watch active threads and RAM usage across PSOCK workers
#'
#' Continuously queries each worker for the number of active threads (via
#' \code{clusters::numActiveThreads()}) and RAM usage (via
#' \code{clusters::ramUsageGB()}) and displays a two-row, aligned status
#' display that updates in place:
#'
#' \preformatted{
#' birds    biomass   camas
#'      12        8       5      <- active threads (right-aligned)
#'  9.3/64  24.1/128  6.2/64    <- used/totalGB   (right-aligned)
#' }
#'
#' Column widths are fixed at startup so that the widest possible RAM string
#' (used == total) never causes columns to shift during the run.
#' On interrupt (\kbd{Ctrl-C}), it prints a final aligned summary of peak
#' thread counts and returns them invisibly.
#'
#' @param cl A PSOCK cluster object (e.g., created with
#'   \code{parallelly::makeClusterPSOCK()}) connected to the remote machines
#'   corresponding to \code{cores}.  If omitted, a cluster is created
#'   automatically and stopped on exit.
#' @param cores A character vector of core (host) names. The order and names
#'   define column layout and are used to name the returned peak vector.
#' @param pad Integer number of spaces to insert \emph{between} columns
#'   (default: \code{2}). Padding is not part of the alignment width.
#' @param interval Numeric number of seconds to wait between refreshes
#'   (default: \code{1}).
#'
#' @details
#' - Thread counts: calls \code{clusters::numActiveThreads()} on each worker.
#' - RAM: calls \code{clusters::ramUsageGB()} on each worker; reads
#'   \file{/proc/meminfo} (Linux only).  Total RAM is fetched once at startup
#'   (it does not change); used RAM is refreshed every tick.
#' - If a worker errors on any metric that tick, the value is shown as
#'   \code{NA} and treated as \code{0} for peak tracking.
#' - Two live rows are updated in-place using ANSI escape sequences.  Run in
#'   RStudio Server or a modern terminal; if ANSI is not supported the escape
#'   codes will appear literally.
#'
#' @return Invisibly returns a named integer vector of peak active thread
#'   counts, with names matching \code{cores}.
#'
#' @section Requirements on workers:
#' The \pkg{clusters} package must be installed on each worker.
#'
#' @examples
#' \dontrun{
#' if (FALSE) {
#' cores <- c("birds", "biomass", "camas", "carbon", "caribou", "coco")
#'
#' cl <- parallelly::makeClusterPSOCK(workers = cores, rshcmd = "ssh",
#'                                    homogeneous = FALSE)
#' peak <- monitorCluster(cl, cores, pad = 2, interval = 1)
#' print(peak)
#' parallel::stopCluster(cl)
#' }
#' }
#'
#' @seealso \code{\link[parallelly]{makeClusterPSOCK}},
#'   \code{\link[parallel]{clusterEvalQ}},
#'   \code{\link[clusters]{numActiveThreads}},
#'   \code{\link[clusters]{ramUsageGB}}
#'
#' @export
monitorCluster <- function(cl, cores, pad = 2, interval = 1) {
  stopifnot(length(cores) > 0)

  if (missing(cl)) {
    cl <- parallelly::makeClusterPSOCK(
      workers = cores,
      rshcmd = "ssh",
      homogeneous = FALSE
    )
    on.exit(parallel::stopCluster(cl))
  }

  master_libpaths <- .libPaths()
  parallel::clusterCall(cl, function(p) .libPaths(p), master_libpaths)
  parallel::clusterEvalQ(cl, { requireNamespace("clusters", quietly = TRUE) })

  # --- Column widths --------------------------------------------------------
  # Fetch total RAM once (it does not change between ticks).
  total_list <- parallel::clusterEvalQ(cl, {
    tryCatch(clusters::ramUsageGB()$total_gb, error = function(e) NA_real_)
  })
  total_ram <- setNames(unlist(total_list, use.names = FALSE), cores)

  # Format a RAM string; worst case is used == total (widest possible string).
  fmt_ram <- function(used, total) {
    if (is.na(used) || is.na(total)) return("?/?GB")
    sprintf("%.1f/%.1fGB", used, total)
  }
  worst_ram_width <- max(nchar(mapply(fmt_ram, total_ram, total_ram)))

  # Column width = max(core name length, widest RAM string).
  col_widths <- pmax(nchar(cores), worst_ram_width)

  # --- Rendering helpers ----------------------------------------------------
  header <- paste(
    mapply(function(nm, w) sprintf("%-*s%s", w, nm, strrep(" ", pad)),
           cores, col_widths),
    collapse = ""
  )

  render_row <- function(vals) {
    # vals is a character vector ordered by cores; NA -> blank
    vals[is.na(vals)] <- ""
    paste(
      mapply(function(v, w) sprintf("%*s%s", w, v, strrep(" ", pad)),
             vals, col_widths),
      collapse = ""
    )
  }

  # --- State ----------------------------------------------------------------
  peak       <- setNames(rep(0L, length(cores)), cores)
  first_tick <- TRUE

  # --- Main loop ------------------------------------------------------------
  cat(header, "\n", sep = "")

  tryCatch({
    repeat {
      # Thread counts
      thread_list <- parallel::clusterEvalQ(cl, {
        tryCatch(clusters::numActiveThreads(), error = function(e) NA_integer_)
      })
      threads <- setNames(unlist(thread_list, use.names = FALSE), cores)

      # Used RAM
      ram_list <- parallel::clusterEvalQ(cl, {
        tryCatch(clusters::ramUsageGB()$used_gb, error = function(e) NA_real_)
      })
      used_ram <- setNames(unlist(ram_list, use.names = FALSE), cores)

      # Update peak
      threads_nona <- threads
      threads_nona[is.na(threads_nona)] <- 0L
      peak <- pmax(peak, threads_nona)

      # Format display values
      thread_strs <- ifelse(is.na(threads[cores]), NA_character_,
                            as.character(threads[cores]))
      ram_strs    <- mapply(fmt_ram, used_ram[cores], total_ram[cores])

      # Draw: on first tick just print; on subsequent ticks move up 1 line
      # first, clearing the threads row, then fall through to clear the RAM row.
      if (first_tick) {
        cat(render_row(thread_strs), "\n", sep = "")
        cat(render_row(ram_strs), sep = "")
        first_tick <- FALSE
      } else {
        cat("\033[A\033[2K\r", render_row(thread_strs), "\n", sep = "")
        cat("\033[2K\r",       render_row(ram_strs),         sep = "")
      }
      flush.console()

      Sys.sleep(interval)
    }
  }, interrupt = function(e) {
    cat("\033[2K\r\n")
    cat("Final PEAKs:\n")
    cat(header, "\n", sep = "")
    cat(render_row(as.character(peak[cores])), "\n", sep = "")
    cat(sprintf("Total peak threads across all cores: %d\n", sum(peak)))
    invisible(peak)
  })
}
