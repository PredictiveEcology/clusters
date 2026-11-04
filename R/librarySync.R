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

## The libraries of `libs` that belong to the user, in order: everything but R's
## own (.Library, .Library.site), which every host has. A run may have a dev
## library first and a frozen stable one behind it; all are mirrored.
.shippableLibs <- function(libs) {
  own <- normalizePath(c(.Library, .Library.site, R.home("library")), mustWork = FALSE)
  libs[!normalizePath(libs, mustWork = FALSE) %in% own]
}

## Mirror each of `libs`, in order, to `host` at the same path. Not `--update`
## and with `--delete`: the master is the truth for what the workers run.
## Stops at the first library that fails.
.syncLibraries <- function(libs, host, rsync, run = .runWithRetry) {
  for (lib in libs) {
    res <- run(rsync, c("-a", "--delete", shQuote(paste0(lib, "/")),
                        shQuote(paste0(host, ":", lib, "/"))))
    if (!identical(res$status, 0L))
      stop("rsync of the project library '", lib, "' to '", host, "' failed (exit ", res$status,
           ") after ", res$tries, " attempt(s): ", res$log, ". ",
           "Workers there would run a different library than the master.")
  }
  invisible(libs)
}

## The lines a worker runs at startup: create the per-user library if needed, then
## put the master's libraries first, in order, and the per-user one after them, so
## nothing installed there shadows the run's libraries (dev before stable).
.workerStartupLines <- function(master_libs) c(
  sprintf('master_libs <- c(%s)', paste(sprintf('"%s"', master_libs), collapse = ", ")),
  'user_lib_raw <- Sys.getenv("R_LIBS_USER")',
  'user_lib <- normalizePath(path.expand(gsub("%v", paste(R.version$major, R.version$minor, sep="."), user_lib_raw)), mustWork = FALSE)',
  'dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)',
  '.libPaths(c(master_libs, user_lib, .libPaths()))'
)
