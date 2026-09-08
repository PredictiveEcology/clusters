## Which of `pkgs` have a DESCRIPTION in any of `libs`. Split out of
## plan_psock_min() so the decisions it drives -- what is mirrored to a host,
## what is reported missing -- are testable without a cluster.
.libraryHas <- function(pkgs, libs) {
  vapply(pkgs, function(p) any(file.exists(file.path(libs, p, "DESCRIPTION"))),
         logical(1), USE.NAMES = FALSE)
}

## Run a command, keeping its output; retry on a non-zero exit. rsync to a
## host can fail transiently (a dropped SSH session, a file replaced under it)
## and used to be run with stderr discarded, so a failure said only "exit 23".
## @return list(status = integer exit status, tries = attempts made,
##   log = the last attempt's combined output, collapsed to one string)
.runWithRetry <- function(cmd, args, tries = 3L, pause = 2) {
  status <- 1L; out <- character()
  for (i in seq_len(tries)) {
    out <- suppressWarnings(system2(cmd, args, stdout = TRUE, stderr = TRUE))
    status <- attr(out, "status"); if (is.null(status)) status <- 0L
    if (identical(as.integer(status), 0L)) return(list(status = 0L, tries = i, log = ""))
    if (i < tries) Sys.sleep(pause * i)
  }
  list(status = as.integer(status), tries = tries,
       log = paste(utils::tail(out, 5), collapse = " | "))
}
