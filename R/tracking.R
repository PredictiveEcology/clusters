#' Watch active threads across PSOCK workers and track per-core peaks
#'
#' Continuously queries each worker for the number of active threads via
#' `clusters::numActiveThreads()` and displays a single, aligned status line
#' that updates in place. It also tracks the **peak** thread count per core
#' (named by the `cores` vector). On interrupt (Ctrl-C), it prints a final
#' aligned summary of peak values and returns them invisibly.
#'
#' The display renders a one-line header of core names (left-aligned),
#' followed by a live one-line status of current counts (right-aligned within
#' the width of each core name). Lines are redrawn in-place using ANSI erase
#' sequences (`\033[2K\r`) which are supported in RStudio Server and most
#' ANSI-aware terminals.
#'
#' @param cl A PSOCK cluster object (e.g., created with
#'   \code{parallelly::makeClusterPSOCK()}) connected to the remote machines
#'   corresponding to \code{cores}.
#' @param cores A character vector of core (host) names. The order and names
#'   define column layout and are used to name the returned peak vector.
#' @param pad Integer number of spaces to insert \emph{between} columns
#'   (default: \code{2}). Padding is not part of the alignment width.
#' @param interval Numeric number of seconds to wait between refreshes
#'   (default: \code{1}).
#'
#' @details
#' - The function calls \code{clusters::numActiveThreads()} on each worker.
#'   If a worker errors, that tick treats the value as \code{NA} for display
#'   and as \code{0} when updating the peak (so peaks are not reduced by \code{NA}s).
#' - The header is printed once. Each subsequent update clears and redraws only
#'   the live values line using ANSI sequences. If ANSI is not supported by the
#'   current console, the escape codes may appear literally; in that case, run
#'   the function in RStudio Server, a modern terminal, or adapt it to use
#'   fallbacks.
#' - The function runs until interrupted (e.g., \kbd{Ctrl-C}). On interrupt, it
#'   clears the live line, prints the final peaks aligned under the header, and
#'   returns the named peak vector invisibly.
#'
#' @return Invisibly returns a named integer vector of peak active thread counts,
#'   with names matching \code{cores}. Also prints a final aligned summary on
#'   interrupt.
#'
#' @section Requirements on workers:
#' The \pkg{clusters} package must be installed on each worker. This function
#' issues \code{parallel::clusterEvalQ(cl, requireNamespace("clusters"))} once
#' to assert availability, but it does not install packages remotely.
#'
#' @examples
#' \dontrun{
#' if (FALSE) {
#' library(parallelly)
#' library(parallel)
#'
#' cores <- c("birds", "biomass", "camas", "carbon", "caribou", "coco",
#'            "core", "dougfir", "fire", "mpb", "sbw", "mega",
#'            "acer", "abies", "pinus")
#'
#' # Create a PSOCK cluster over SSH (example; configure to your environment)
#' cl <- parallelly::makeClusterPSOCK(
#'  workers = cores,
#'   rshcmd = "ssh",
#'    homogeneous = FALSE
#'  )
#'
#' # Watch and track peaks; press Ctrl-C to stop.
#' peak <- monitorCluster(cl, cores, pad = 2, interval = 1)
#'
#' # After interrupt, 'peak' is a named integer vector with per-core maxima.
#' print(peak)
#'
#' stopCluster(cl)
#' }
#' }
#'
#' @seealso \code{\link[parallelly]{makeClusterPSOCK}},
#'   \code{\link[parallel]{clusterEvalQ}},
#'   \code{\link[clusters]{numActiveThreads}}
#'
#' @note
#' This function uses ANSI escape sequences to clear and redraw a single line
#' (\code{"\\033[2K\\r"}). RStudio Server consoles interpret ANSI by default.
#' If your output is redirected (non-TTY) or ANSI is disabled, consider
#' adapting the clearing step to use a fallback (e.g., overwrite with spaces).
#'
#' @export
monitorCluster <- function(cl, cores, pad = 2, interval = 1) {
  stopifnot(length(cores) > 0)

  if (missing(cl)) {
    cl <- parallelly::makeClusterPSOCK(
      workers = cores ,
      rshcmd = "ssh",
      homogeneous = FALSE
    )
    on.exit(parallel::stopCluster(cl))

  }
  # Ensure 'clusters' is available on workers (optional but helpful)
  master_libpaths <- .libPaths()
  parallel::clusterCall(cl, function(p) .libPaths(p), master_libpaths)

  parallel::clusterEvalQ(cl, { requireNamespace("clusters", quietly = TRUE) })

  # Widths equal the name lengths; pad is spacing BETWEEN columns
  name_widths <- nchar(cores)

  # Header (left-aligned names + pad spaces between columns)
  header <- paste(
    mapply(function(name, w) sprintf("%-*s%s", w, name, strrep(" ", pad)),
           cores, name_widths),
    collapse = ""
  )
  cat(header, "\n", sep = "")

  # Render one values line: right-align within name width, then add pad
  render_values_line <- function(vals_named) {
    vals <- vals_named[cores]
    vals[is.na(vals)] <- ""
    paste(
      mapply(function(val, w) sprintf("%*s%s", w, val, strrep(" ", pad)),
             vals, name_widths),
      collapse = ""
    )
  }

  # Peak tracker (named numeric vector)
  peak <- setNames(rep(0L, length(cores)), cores)

  # Main loop — Ctrl-C to stop; on interrupt, print final peaks and return them
  tryCatch({
    repeat {
      # Evaluate clusters::numActiveThreads() directly on each worker
      res_list <- parallel::clusterEvalQ(cl, {
        # Be robust to errors per node
        tryCatch(clusters::numActiveThreads(), error = function(e) NA_integer_)
      })

      # Format into a named vector in the same order as 'cores'
      vals <- unlist(res_list, use.names = FALSE)
      names(vals) <- cores

      # Update peak (treat NA as 0 for peak comparison)
      vals_non_na <- vals
      vals_non_na[is.na(vals_non_na)] <- 0L
      peak <- pmax(peak, vals_non_na)

      # Clear the values line & redraw (RStudio Server: ANSI supported)
      cat("\033[2K\r")
      cat(render_values_line(vals))
      flush.console()

      Sys.sleep(interval)
    }
  }, interrupt = function(e) {
    # On Ctrl-C: clear live line, print final PEAKs aligned, and return 'peak'
    cat("\033[2K\r\n")
    cat("Final PEAKs:\n")
    cat(header, "\n", sep = "")
    cat(render_values_line(peak), "\n", sep = "")
    cat(sprintf("Total peak threads across all cores: %d\n", sum(peak)))
    invisible(peak)
  })
}
