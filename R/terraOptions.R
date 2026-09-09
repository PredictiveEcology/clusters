#' Give cluster workers the master's terra memory settings
#'
#' A PSOCK worker is a fresh R session, so it starts at terra's defaults no matter what
#' the master set: `memfrac = 0.5`, `memmax = 16`, `todisk = FALSE`. `memfrac` is a
#' fraction of the machine's *total* RAM and terra applies it per process, so a hundred
#' workers each sizing a working buffer at half of total RAM is not a policy anyone
#' chose -- and it silently overrides whatever the master was careful to set.
#'
#' Called by [clusterSetup()] once the workers are up. Does nothing when terra is not
#' installed, on either side.
#'
#' @param cl A cluster object, as returned by [parallel::makeCluster()] and friends.
#' @return Invisibly, the named list of settings sent, or `NULL` if terra is unavailable
#'   on the master.
#' @export
#' @examples
#' \donttest{
#' if (requireNamespace("terra", quietly = TRUE)) {
#'   cl <- parallel::makeCluster(1L)
#'   terra::terraOptions(memfrac = 0, todisk = TRUE)
#'   mirrorTerraOptions(cl)
#'   parallel::clusterEvalQ(cl, terra::terraOptions(print = FALSE)$memfrac) # 0
#'   parallel::stopCluster(cl)
#' }
#' }
mirrorTerraOptions <- function(cl) {
  if (!requireNamespace("terra", quietly = TRUE)) {
    return(invisible(NULL))
  }
  o <- terra::terraOptions(print = FALSE)
  vals <- list(memfrac = o$memfrac, memmax = o$memmax, todisk = o$todisk)
  vals <- vals[!vapply(vals, is.null, logical(1))]
  if (!length(vals)) {
    return(invisible(NULL))
  }

  parallel::clusterCall(cl, function(vals) {
    ## A host without terra is not an error here: it simply cannot run terra work.
    if (requireNamespace("terra", quietly = TRUE)) {
      do.call(terra::terraOptions, vals)
    }
    invisible(NULL)
  }, vals)

  invisible(vals)
}
