
## Decide what to ship to one host. Split out of shipSystemLibs() so the policy
## -- refuse on platform mismatch, never ship the core runtime, skip anything the
## master cannot resolve -- is testable without standing up a cluster.
## @return list(ship = <sonames to copy>, unmet = <sonames we will not/cannot>)
.planSystemLibShipment <- function(missing, hostId, masterId,
                                   coreLibs = .coreLibPattern,
                                   resolve = .resolveSoname) {
  if (!length(missing)) return(list(ship = character(0), unmet = character(0)))
  if (!identical(hostId, masterId))
    return(list(ship = character(0), unmet = missing))

  isCore <- grepl(coreLibs, missing)
  unmet <- missing[isCore]
  cand <- missing[!isCore]

  if (length(cand)) {
    src <- vapply(cand, resolve, character(1))
    unmet <- c(unmet, cand[is.na(src)])
    cand <- cand[!is.na(src)]
  }
  list(ship = cand, unmet = unmet)
}

## Never ship these: the host's own must win. See ?shipSystemLibs.
## `[.-]` not `\\.`: the loader is `ld-linux-x86-64.so.2`, so requiring a dot
## immediately after the name let it through. The character class still keeps
## libcurl/libmagick++ out of the libc/libm buckets.
.coreLibPattern <- "^(ld-linux|libc|libstdc\\+\\+|libgcc_s|libm|libpthread|libdl|librt)[.-]"

#' Ship missing system shared libraries to worker hosts, without root
#'
#' @description
#' The project library is rsynced to each host, but the *system* libraries those
#' compiled packages link against are not. A host missing one fails at load with
#' e.g. `libtbb.so.12: cannot open shared object file` -- RcppParallel, and so
#' everything that depends on it. The obvious fix, `sudo apt-get install`, needs
#' root on every host, which is exactly what a shared research cluster tends not
#' to give you.
#'
#' It is not needed. A shared library is just a file: copy it into a
#' user-writable directory on the host and point `LD_LIBRARY_PATH` at it. This
#' detects what is actually missing, ships only that, and returns the directory
#' so the caller can add it to the workers' environment.
#'
#' @section What is deliberately not shipped:
#' The core runtime -- `libc`, `libstdc++`, `libgcc_s`, `libm`, `libpthread`,
#' `libdl`, `librt` and the loader itself. Those must be the host's own: they are
#' what everything else on the machine is already linked against, and overriding
#' them via `LD_LIBRARY_PATH` breaks far more than it fixes. If one of those is
#' genuinely missing, the host is not usable and should be reported, not patched.
#'
#' @section When this refuses:
#' Only when the master and the host agree on architecture and libc version is
#' copying a binary between them sound. Hosts that disagree are skipped with a
#' message rather than given a library that cannot load, or worse, one that can.
#'
#' @param cl A running cluster with one worker per host (the probe cluster).
#' @param hosts Character vector of host labels, in the same order as `cl`.
#' @param libs Library path(s) whose compiled code should be checked.
#' @param dest Directory on each host to copy into. Must be user-writable;
#'   defaults to `~/.local/lib/clusters`.
#' @param verbose Logical; report what is shipped.
#'
#' @return Invisibly, a named list with `dest` (as given), `destResolved` (the
#'   absolute directory on each host), `ldPath` (each host's existing
#'   `LD_LIBRARY_PATH`), `shipped` (per host, the sonames copied) and `unmet`
#'   (per host, anything still missing that this will not ship).
#'
#' @section Setting `LD_LIBRARY_PATH`:
#' It must be in the worker's environment **before R starts**. glibc parses
#' `LD_LIBRARY_PATH` once at process startup, so `Sys.setenv()` from inside R --
#' which is what `parallelly`'s `rscript_envs` does, via
#' `Rscript -e 'Sys.setenv(...)'` -- has no effect on later `dlopen()`. Verified:
#' with the library shipped but only `Sys.setenv()`, `library(fireSenseUtils)`
#' still fails; with `env LD_LIBRARY_PATH=... Rscript`, it loads. Prefix the
#' worker command instead.
#' @export
shipSystemLibs <- function(cl, hosts, libs, dest = "~/.local/lib/clusters",
                           verbose = TRUE) {
  masterId <- .platformId()

  parallel::clusterExport(cl, c("libs", "dest"), envir = environment())

  probe <- parallel::clusterEvalQ(cl, {
    id <- list(arch = R.version$arch,
               libc = tryCatch(
                 sub(".*?([0-9]+\\.[0-9]+)\\s*$", "\\1",
                     system2("ldd", "--version", stdout = TRUE)[1]),
                 error = function(e) NA_character_))
    ## Ask the loader directly rather than inferring from a failed library():
    ## a package can fail to load for reasons that have nothing to do with a
    ## missing DSO, and one missing DSO can break many packages at once.
    sos <- list.files(libs, pattern = "\\.so$", recursive = TRUE, full.names = TRUE)
    sos <- grep("/libs/", sos, value = TRUE)
    missing <- unlist(lapply(sos, function(f) {
      out <- suppressWarnings(system2("ldd", shQuote(f), stdout = TRUE, stderr = FALSE))
      sub("^\\s*([^ ]+).*", "\\1", grep("not found", out, value = TRUE))
    }))
    list(id = id, missing = unique(missing),
         dest = normalizePath(path.expand(dest), mustWork = FALSE),
         ldPath = Sys.getenv("LD_LIBRARY_PATH"))
  })

  shipped <- unmet <- stats::setNames(vector("list", length(hosts)), hosts)

  for (i in seq_along(hosts)) {
    need <- probe[[i]]$missing
    if (!length(need)) next

    if (verbose && !identical(probe[[i]]$id, masterId))
      message("Not shipping system libraries to ", hosts[i],
              ": it is ", probe[[i]]$id$arch, "/glibc ", probe[[i]]$id$libc,
              " and this master is ", masterId$arch, "/glibc ", masterId$libc,
              ". Missing: ", paste(need, collapse = ", "))

    plan <- .planSystemLibShipment(need, probe[[i]]$id, masterId)
    unmet[[i]] <- c(unmet[[i]], plan$unmet)
    need <- plan$ship
    if (!length(need)) next
    src <- vapply(need, .resolveSoname, character(1))

    remoteDest <- probe[[i]]$dest
    system2("ssh", c("-o", "BatchMode=yes", hosts[i],
                     shQuote(paste("mkdir -p", shQuote(remoteDest)))),
            stdout = FALSE, stderr = FALSE)
    ## -L: copy what the symlink points at, under the soname the loader wants.
    for (j in seq_along(src)) {
      st <- system2("rsync", c("-aL", shQuote(src[[j]]),
                               shQuote(paste0(hosts[i], ":", file.path(remoteDest, need[[j]])))),
                    stdout = FALSE, stderr = FALSE)
      if (identical(as.integer(st), 0L)) shipped[[i]] <- c(shipped[[i]], need[[j]])
      else unmet[[i]] <- c(unmet[[i]], need[[j]])
    }
    if (verbose && length(shipped[[i]]))
      message("Shipped to ", hosts[i], ":", remoteDest, " -- ",
              paste(shipped[[i]], collapse = ", "))
  }

  if (verbose && length(unlist(unmet)))
    message("Still missing (not shippable): ",
            paste(unique(unlist(unmet)), collapse = ", "))

  invisible(list(dest = dest,
                 destResolved = vapply(probe, `[[`, character(1), "dest"),
                 ldPath = vapply(probe, `[[`, character(1), "ldPath"),
                 shipped = shipped, unmet = unmet))
}

## arch + libc version: the two things that decide whether a binary compiled
## here can be loaded there.
.platformId <- function() {
  list(arch = R.version$arch,
       libc = tryCatch(
         sub(".*?([0-9]+\\.[0-9]+)\\s*$", "\\1",
             system2("ldd", "--version", stdout = TRUE)[1]),
         error = function(e) NA_character_))
}

## Where does this master's loader find <soname>?
.resolveSoname <- function(soname) {
  out <- suppressWarnings(system2("ldconfig", c("-p"), stdout = TRUE, stderr = FALSE))
  hit <- grep(paste0("^\\s*", gsub("\\.", "\\\\.", soname), "\\s"), out, value = TRUE)
  if (!length(hit)) return(NA_character_)
  p <- sub(".*=>\\s*", "", hit[[1]])
  if (file.exists(p)) p else NA_character_
}
