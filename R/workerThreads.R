## OpenBLAS threads in cluster workers
##
## R linked to a multithreaded OpenBLAS starts one thread per logical CPU (up to 64) when
## R starts, and every matrix product then wakes the whole pool. In a cluster that already
## runs one worker per CPU this only adds contention: on the FireSense fleet (2026-09-15)
## each DEoptim worker held 49 threads and used ~6 CPUs while the allocator counted one,
## and a 50,000 x 12 product ran 2-3x slower with the pool than with one thread.
##
## The variable must be in the environment when R starts: parallelly's `rscript_envs`
## sets variables with Sys.setenv() after R has started, by which time OpenBLAS has
## already created its threads (64 threads with rscript_envs, 1 with an `env` prefix).

#' Prefix a worker command with an OpenBLAS thread cap
#'
#' @param rscript The command that starts a worker, as passed to
#'   [parallelly::makeClusterPSOCK()] (`"Rscript"`, or a vector that already starts
#'   with `"env"`).
#' @param blasThreads Number of OpenBLAS threads per worker. `NA` or `NULL` leaves the
#'   command alone. Defaults to `getOption("clusters.workerBlasThreads", 1L)`.
#' @return `rscript`, prefixed with `env OPENBLAS_NUM_THREADS=<blasThreads>`.
#' @keywords internal
.workerRscript <- function(rscript, blasThreads = getOption("clusters.workerBlasThreads", 1L)) {
  if (is.null(blasThreads) || length(blasThreads) != 1L || is.na(blasThreads))
    return(rscript)
  c("env", paste0("OPENBLAS_NUM_THREADS=", as.integer(blasThreads)), rscript)
}
