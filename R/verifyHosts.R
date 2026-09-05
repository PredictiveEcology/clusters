#' Verify that every cluster host can actually run a worker
#'
#' @description
#' `plan_psock_min()` rsyncs the master's library to the same absolute path on
#' each host and prepends it. Three things can silently go wrong with that, and
#' each produces a worker that dies later rather than a build that fails now:
#'
#' * **R version drift.** A library built under one R major.minor is ignored by
#'   another, which then falls back to the system library and cannot find the
#'   packages at all. (`plan_psock_min()` has always checked this one.)
#' * **Package version drift.** The rsync only carries the dependency closure of
#'   `pkgsNeeded`, so anything outside it can differ between master and worker --
#'   including the packages doing the orchestrating.
#' * **A library that is present but unloadable**, typically a missing system
#'   library (`libtbb.so.12` for RcppParallel, `libgdal.so.NN` for sf/terra) or a
#'   binary built against a different GDAL.
#'
#' This checks all three against the master and reports every offending host at
#' once, rather than failing on the first or -- worse -- succeeding and letting
#' the workers die one at a time inside the objective function.
#'
#' @param cl A running cluster with one worker per host (the probe cluster).
#' @param hosts Character vector of host labels, in the same order as `cl`.
#' @param pkgs Character vector of packages that a worker must be able to load.
#' @param libs Library paths to prepend on each worker (the master's).
#' @param action `"stop"` (default) to error listing the bad hosts, or `"drop"`
#'   to return only the hosts that passed.
#'
#' @return Character vector of hosts that passed, invisibly. With
#'   `action = "stop"` that is always `hosts`.
#' @export
verifyClusterHosts <- function(cl, hosts, pkgs, libs, action = c("stop", "drop")) {
  action <- match.arg(action)

  masterR <- paste(R.version$major,
                   strsplit(R.version$minor, ".", fixed = TRUE)[[1L]][1L], sep = ".")
  masterPkg <- vapply(pkgs, function(p)
    tryCatch(as.character(utils::packageVersion(p)), error = function(e) NA_character_),
    character(1))
  masterGDAL <- tryCatch(sf::sf_extSoftVersion()[["GDAL"]], error = function(e) NA_character_)

  parallel::clusterExport(cl, c("pkgs", "libs"), envir = environment())
  got <- parallel::clusterEvalQ(cl, {
    .libPaths(unique(c(libs, .libPaths())))
    list(
      R = paste(R.version$major,
                strsplit(R.version$minor, ".", fixed = TRUE)[[1L]][1L], sep = "."),
      # Load, do not merely look up: a present-but-unloadable package (missing
      # system library, wrong GDAL ABI) is the failure this is here to catch.
      loaded = vapply(pkgs, function(p)
        isTRUE(tryCatch({loadNamespace(p); TRUE}, error = function(e) FALSE)), logical(1)),
      versions = vapply(pkgs, function(p)
        tryCatch(as.character(utils::packageVersion(p)), error = function(e) NA_character_),
        character(1)),
      GDAL = tryCatch(sf::sf_extSoftVersion()[["GDAL"]], error = function(e) NA_character_)
    )
  })

  problems <- character(0)
  ok <- rep(TRUE, length(hosts))
  for (i in seq_along(hosts)) {
    g <- got[[i]]
    why <- character(0)
    if (!identical(g$R, masterR))
      why <- c(why, paste0("R ", g$R, " != master ", masterR))
    bad <- names(g$loaded)[!g$loaded]
    if (length(bad))
      why <- c(why, paste0("cannot load: ", paste(bad, collapse = ", ")))
    drift <- names(masterPkg)[!is.na(masterPkg) & !is.na(g$versions) &
                                masterPkg != g$versions]
    if (length(drift))
      why <- c(why, paste0("version drift: ",
                           paste0(drift, " ", g$versions[drift], " != ", masterPkg[drift],
                                  collapse = "; ")))
    if (!is.na(masterGDAL) && !is.na(g$GDAL) && !identical(g$GDAL, masterGDAL))
      why <- c(why, paste0("GDAL ", g$GDAL, " != master ", masterGDAL))
    if (length(why)) {
      ok[i] <- FALSE
      problems <- c(problems, paste0("  ", hosts[i], ": ", paste(why, collapse = "; ")))
    }
  }

  if (length(problems)) {
    msg <- paste0("Cluster hosts that cannot run a worker:\n", paste(problems, collapse = "\n"))
    if (identical(action, "stop"))
      stop(msg, "\nFix the hosts, or call with action = 'drop' to run without them.",
           call. = FALSE)
    message(msg, "\nDropping them and continuing.")
  }
  invisible(hosts[ok])
}
