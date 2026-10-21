## options(clusters.maxWorkersPerHost): the most workers a host carries, every cluster together, set by the user (a
## shared machine keeps its cores free). Every decision of workers per host reads it here: a build and a
## rebalance through .fitCapacity() (and so .allocateWhenAvailable()), and a repeated-host `cores` vector
## through .trimToHostCap().

## The ssh `Host` aliases of this machine: the Host lines of ~/.ssh/config that name its nodename in a comment.
## changeNodenameToLocalhost() renames them to "localhost" in `cores`.
.localAliases <- function(sshConfig = "~/.ssh/config") {
  if (!file.exists(sshConfig)) return(character(0))
  sshLines <- readLines(sshConfig)
  onlyHost <- grep("^Host ", grep(Sys.info()["nodename"], sshLines, value = TRUE), value = TRUE)
  gsub("^Host (\\w+).*", "\\1", onlyHost)
}

#' The most workers each host may get
#'
#' Reads `options(clusters.maxWorkersPerHost)`: named elements cap those hosts, an unnamed value caps every
#' host, and a named element overrides the unnamed one for its host. A cap named for this machine (its ssh
#' alias, such as `mega`) also applies to it as `"localhost"`, the name it has in `cores` after
#' `changeNodenameToLocalhost()`. The cap is the host's, for every cluster together: `.fitCapacity()` takes
#' off it what other clusters have booked there.
#'
#' @param hosts Host names, as in `cores`.
#' @param caps The option value.
#' @return Numeric, one per host: its cap, `Inf` when it has none.
#' @keywords internal
.maxWorkersPerHost <- function(hosts, caps = getOption("clusters.maxWorkersPerHost")) {
  hosts <- as.character(hosts)
  out <- rep(Inf, length(hosts))
  if (!length(caps)) return(out)
  nm <- names(caps); if (is.null(nm)) nm <- rep("", length(caps))
  out[] <- suppressWarnings(min(Inf, caps[!nzchar(nm)]))
  named <- nzchar(nm)
  for (i in which(named)) {
    mine <- hosts == nm[i] | (hosts == "localhost" & nm[i] %in% c(.localAliases(), Sys.info()[["nodename"]]))
    out[mine] <- caps[[i]]
  }
  out
}

## `cores`, a host name per worker, with each host cut to its cap; a message names the hosts cut.
.trimToHostCap <- function(cores) {
  cap <- stats::setNames(.maxWorkersPerHost(unique(cores)), unique(cores))
  keep <- stats::ave(seq_along(cores), cores, FUN = seq_along) <= cap[cores]
  if (!all(keep)) {
    cut <- table(cores[!keep])
    message("options(clusters.maxWorkersPerHost): ",
            paste0(names(cut), " cut by ", cut, " to ", cap[names(cut)], collapse = ", "), " worker(s)")
  }
  cores[keep]
}
