#' Setup a cluster for DEoptim
#'
#' This includes copying files over to unique(cores) machines, then loading all objects from disk in
#' each of the parallel cores.
#'
#' @export
#' @returns A list of items that can be passed to `DEoptim.control()`
#'
#'
clusterSetup <- function(messagePrefix = "DEoptim_",
                         itermax = 500, trace = TRUE, strategy = 3, initialpop = NULL, NP = NULL,
                         cores, logPath, libPath, objsNeeded, pkgsNeeded, envir = parent.frame()) {

  if (!all(requireNamespace("qs") && requireNamespace("reproducible") && requireNamespace("Require")))
    stop("Please install missing packages")
  control <- list(itermax = itermax, trace = trace, strategy = strategy)

  if (!is.null(initialpop)) {
    control$initialpop <- initialpop
  }

  if (!is.null(NP)) {
    control$NP <- NP
  } else {
    control$NP <- length(cores)
  }

  if (!is.null(cores)) {
    logPath <- file.path(
      logPath,
      paste0(
        messagePrefix, format(Sys.time(), "%Y-%m-%d_%H%M%S"),
        "_pid", Sys.getpid(), ".log"
      )
    )
    message(paste0(
      "Starting parallel model fitting for ",
      messagePrefix, ". Log: ", logPath
    ))

    # Make sure logPath can be written in the workers -- need to create the dir

    if (is.numeric(cores)) cores <- rep("localhost", cores)

    ## Make cluster with just one worker per machine --> don't need to do these steps
    #     multiple times per machine, if not all 'localhost'
    revtunnel <- FALSE
    allLocalhost <- identical("localhost", unique(cores))
    # if (!identical("localhost", unique(cores))) {
    aa <- Require::pkgDep(unique(c("qs", "RCurl", pkgsNeeded)), recursive = TRUE)
    pkgsNeeded <- unique(Require::extractPkgName(unname(unlist(aa))))

    if (!allLocalhost) {
      repos <- c("https://predictiveecology.r-universe.dev", getOption("repos"))

      # FireSense needed "dqrng", "SpaDES.tools", "fireSenseUtils", "PredictiveEcology/fireSenseUtils@development",

      revtunnel <- ifelse(allLocalhost, FALSE, TRUE)

      coresUnique <- setdiff(unique(cores), "localhost")
      message("copying packages to: ", paste(coresUnique, collapse = ", "))

      # RscriptPath = "/usr/local/bin/Rscript"

      # st <- system.time(
      #   cl <- mirai::make_cluster(
      #     n = length(coresUnique),
      #     url = "tcp://localhost:5563",
      #     remote = mirai::ssh_config(
      #       remotes = paste0("ssh://", coresUnique),
      #       tunnel = TRUE,
      #       timeout = 1,
      #       rscript = RscriptPath
      #     )
      #   )
      # )
      st <- system.time({
        cl <- parallelly::makeClusterPSOCK(coresUnique, revtunnel = revtunnel, rscript_libs = libPath
                                           # , rscript = c("nice", RscriptPath)
        )
      })
      clusterExport(cl, list("libPath", "logPath", "repos", "pkgsNeeded"),
                    envir = environment())

      # Missing `dqrng` and `sitmo`
      Require::Install(pkgsNeeded, libPaths = libPath)

      parallel::clusterEvalQ(cl, {
        # If this is first time that packages need to be installed for this user on this machine
        #   there won't be a folder present that is writable
        if (!dir.exists(libPath)) {
          dir.create(libPath, recursive = TRUE)
        }
      })

      message("Setting up packages on the cluster...")
      out <- lapply(setdiff(unique(cores), "localhost"), function(ip) {
        rsync <- Sys.which("rsync")
        if (!nzchar(rsync)) stop()
        system(paste0(rsync, " -aruv --update ", paste(file.path(libPath, pkgsNeeded), collapse = " "),
                      " ", ip, ":", libPath))
      })

      parallel::clusterEvalQ(cl, {
        # If this is first time that packages need to be installed for this user on this machine
        #   there won't be a folder present that is writable
        if (tryCatch(packageVersion("Require") < "1.0.1", error = function(e) TRUE))
          install.packages("Require", lib = libPath)
        library(Require, lib.loc = libPath)
        dir.create(dirname(logPath), recursive = TRUE, showWarnings = FALSE)
        out <- Require::Install(pkgsNeeded, libPaths = libPath)
      })
      GDALversions <- parallel::clusterEvalQ(cl, {
        .libPaths(libPath)
        return(sf::sf_extSoftVersion()["GDAL"])
      })
      stopifnot(length(unique(sf::sf_extSoftVersion()["GDAL"], GDALversions)) == 1)

      parallel::stopCluster(cl)
    }

    dir.create(dirname(logPath), recursive = TRUE, showWarnings = FALSE)

    ## Now make full cluster with one worker per core listed in "cores"
    message("Starting ", paste(paste(names(table(cores))), "x", table(cores),
                               collapse = ", "), " clusters")
    message("Starting main parallel cluster ...")
    # sshCores <- paste0("ssh//", grep('localhost', cores, invert = TRUE, value = TRUE))
    # nonsshCores <- grep('localhost', cores, value = TRUE)
    # coresForMirai <- c(nonsshCores, sshCores)

    st <- system.time({
      # cl <- mirai::make_cluster(
      #   length(coresForMirai),
      #   # url = "tcp://localhost:5555",
      #   remote = ssh_config(
      #     remotes = coresForMirai,
      #     # tunnel = TRUE,
      #     timeout = 1,
      #     rscript = RscriptPath
      #   )
      # )

      cl <- parallelly::makeClusterPSOCK(cores,
                                         revtunnel = revtunnel,
                                         outfile = logPath, rscript_libs = libPath
                                         # , rscript = c("nice", RscriptPath)
      )
    })
    on.exit(stopCluster(cl))

    message(
      "it took ", round(st[3], 2), "s to start ",
      paste(paste(names(table(cores))), "x", table(cores), collapse = ", "), " threads"
    )
    message("Moving objects to each node in cluster")

    stMoveObjects <- try({
      system.time({
        objsToCopy <- mget(unlist(objsNeeded), envir = envir)
        FileBackendsToCopy <- Filenames(objsToCopy)
        hasFilename <- nzchar(FileBackendsToCopy)
        if (any(hasFilename)) {
          objsToMem <- names(FileBackendsToCopy)[hasFilename]
          objsToCopy[objsToMem] <-
            lapply(objsToCopy[objsToMem],
                   function(x) toMemory(x))
        }
        objsToCopy <- reproducible::.wrap(objsToCopy)

        # objsToCopy <- lapply(objsToCopy, FUN = function(x) {
        #   if (inherits(x, "SpatRaster")) {
        #     x <- reproducible::.wrap(x)
        #   } else {
        #     x
        #   }
        #   x
        # })
        filenameForTransfer <- normalizePath(tempfile(fileext = ".qs"), mustWork = FALSE, winslash = "/")
        dir.create(dirname(filenameForTransfer), recursive = TRUE, showWarnings = FALSE) # during development, this was deleted accidentally
        qs::qsave(objsToCopy, file = filenameForTransfer)
        stExport <- system.time({
          outExp <- clusterExport(cl, varlist = "filenameForTransfer", envir = environment())
        })
        out11 <- clusterEvalQ(cl, {
          dir.create(dirname(filenameForTransfer), recursive = TRUE, showWarnings = FALSE)
        })
        nonLocalhostCores <- setdiff(unique(cores), "localhost")
        if (length(nonLocalhostCores))
          out <- lapply(nonLocalhostCores, function(ip) {
            rsync <- Sys.which("rsync")
            st1 <- system.time(system(paste0(rsync, " -av ",
                                             filenameForTransfer, " ", ip, ":",
                                             filenameForTransfer)))
          })
        out <- clusterEvalQ(cl, {
          out <- qs::qread(file = filenameForTransfer)
          out <- reproducible::.unwrap(out, cachePath = NULL)
          # out <- lapply(out, FUN = function(x) {
          #   if (inherits(x, "PackedSpatRaster")) {
          #     x <- terra::unwrap(x)
          #   } else {
          #     x
          #   }
          #   x
          # })
          list2env(out, envir = .GlobalEnv)
        })
        # Delete the file
        notDups <- !duplicated(cores)
        out <- clusterEvalQ(cl[notDups], {
          if (dir.exists(dirname(filenameForTransfer))) {
            try(unlink(dirname(filenameForTransfer), recursive = TRUE), silent = TRUE)
          }
        })
      })
    })

    if (is(stMoveObjects, "try-error")) {
      message("The attempt to move objects to cluster using rsync and qs failed; trying clusterExport")
      stMoveObjects <- system.time(clusterExport(cl, objsNeeded, envir = environment()))
      list2env(mget(unlist(objsNeeded), envir = environment()), envir = .GlobalEnv)
    }
    message("it took ", round(stMoveObjects[3], 2), "s to move objects to nodes")

    message("loading packages in cluster nodes")

    clusterExport(cl, "pkgsNeeded", envir = environment())
    stPackages <- system.time(parallel::clusterEvalQ(
      cl,
      {
        for (i in pkgsNeeded) {
          library(i, character.only = TRUE)
        }
        message("loading ", i, " at ", Sys.time())
      }
    ))
    message("it took ", round(stPackages[3], 2), "s to load packages")

    control$cluster <- cl
    if (identical(cores, "localhost"))
      list2env(mget(unlist(objsNeeded), envir = envir), envir = .GlobalEnv)
  }

  on.exit() # remove on.exit stopCluster because it ended successfully

  control
}




#'
#' This is for cleaning up cases where an interrupted optimization
#' is leading to multiple files for the same .runName. This will remove
#' duplicates, keeping only the most recent.
#'
#' @return For side effects: removed files
#' @export
#' @param path A folder in which to search for duplicates
#' @param pattern The regular expression to search for, to identify the files. This
#'   must have 1 set of parentheses (), as only the content between the () will be
#'   used for duplicate assessment, i.e., remove anything in the file that shouldn't
#'   be used.
#' @param delete Logical. Default `FALSE`, which will only list the files that
#'   will be deleted. If `TRUE`, then the identified files will
#'   be deleted
rmIncompleteDups <- function(path, pattern = "^(.+)\\_[[:digit:]]{5,8}.*\\.png",
                             delete = FALSE) {
  d <- dir(path, recursive = TRUE, full.names = TRUE);
  e <- file.info(d)
  ord <- order(e$mtime, decreasing = TRUE)
  fls <- d[ord]
  fls2 <- gsub(pattern, "\\1", fls)
  dups <- duplicated(fls2)
  fls3 <- unique(fls[dups])
  if (isTRUE(delete))
    unlink(fls3)
  if (length(fls3)) {
    filesToDelete <- paste(fls3, collapse = "\n")
    if (isTRUE(delete))
      message("removed: ", filesToDelete)
    else
      message("would be removed: ", filesToDelete)
  }
  else
    message("None to remove")
  invisible(fls3)
}


#' Assess the machine resources used
#'
#' Uses `vmstat` (must be installed; it is by default on linux).
#'
#' @return Returns the outputs from `vmstat`
#' @export
#' @param machines Character vector of the name(s) of the PSOCK resource to
#'   query, e.g., `"n168"`
#' @param resources Column extracted in vmstat. Defaults to `"us"` or "user CPU"
resourcesUsed <- function(machines = "localhost", resource = "us") {
  #if (!identical("localhost", machines)) {
  cl <- suppressMessages(parallelly::makeClusterPSOCK(machines))
  on.exit(parallel::stopCluster(cl))
  out <- parallel::clusterEvalQ(cl, {
    a <- system("vmstat -y", intern = TRUE)[-1]
    a <- gsub("^ +", "", a)
    asplit <- strsplit(a, " +")
    wh <- which(asplit[[1]] == "us")
    out <- as.numeric(asplit[[2]][wh])
  })
  #}
  names(out) <- machines
  unlist(out)
}

#' Remove duplicate figures, keeping most recent duplicate only
#'
#' This is for cleaning up cases where an interrupted optimization
#' is leading to multiple files for the same .runName. This will remove
#' duplicates, keeping only the most recent.
#'
#' @return For side effects: removed files
#' @export
#' @param path A folder in which to search for duplicates
#' @param pattern The regular expression to search for, to identify the files. This
#'   must have 1 set of parentheses (), as only the content between the () will be
#'   used for duplicate assessment, i.e., remove anything in the file that shouldn't
#'   be used.
#' @param delete Logical. Default `FALSE`, which will only list the files that
#'   will be deleted. If `TRUE`, then the identified files will
#'   be deleted
dirNew <- function(path, secsAgo = Inf, after = Sys.time() - secsAgo,
                   pattern = "^(.+)\\_[[:digit:]]{6,8}.*\\.png") {
  d <- dir(path, recursive = TRUE, full.names = TRUE);
  e <- file.info(d)
  ord <- order(e$mtime, decreasing = TRUE)
  newer <- e$mtime >= after
  fls <- d[newer]
  # fls <- d[ord]
  fls2 <- grep(pattern, fls, value = TRUE)
  fls2
}


#' Tabulate the filenames into groups based on `pattern`
#'
#' Using a regular expression, with a single `(.+)` identifying the parts of
#' the filenames to keep, and therefore to base the `table` on. Everything
#' before
#'
#' @return Tabulation of the files, based on the pattern.
#' @export
#' @param files A vector of full filenames
#' @param pattern The regular expression to base the `table` on. It should have
#'   one and only one parenthesis i.e., `(.+)`, which will be the basis of
#'   the `table`. Everything outside of the `(.+)` will be removed
tableFiles <- function(files, pattern = "^.+hists/(.+)\\_iter.+\\_[[:digit:]]{6,8}.*\\.png") {
  # dd <- dirNew(path, secsAgo, pattern = "hist.+MPB\\_4")
  files <- sort(files)
  table(gsub(pattern, "\\1", files))
}


#' Summary -- wrapper around `dirNew` and `tableFiles`
#'
#' Convenient wrapper.
#'
#' @return Tabulation of the files, based on the pattern.
#' @export
#' @inheritParams tableFiles
#' @inheritParams dirNew
summaryOutputFolder <- function(path, pattern = "^.+hists/(.+)\\_iter.+\\_[[:digit:]]{6,8}.*\\.png") {
  dd <- clusters::dirNew(path, pattern = pattern)
  fi <- file.info(dd) |> as.data.table() # |> sort(by = "mtime")
  ordFi <- order(fi$mtime)
  setorderv(fi, "mtime")
  dt <- difftime( Sys.time(), fi$mtime[1], units = "hours") #/ NROW(dd)
  print(dt)
  print(paste(NROW(dd)/as.numeric(dt), "runs per hour"))
  tf <- sort(tableFiles(dd, pattern = pattern))
  tf <- tf[order(names(tf))]
  dt2 <- difftime(Sys.time(), fi$mtime, units = "mins")
  ddNew <- dd[ordFi][dt2 < 20]
  tfNew <- tableFiles(ddNew, pattern = pattern)
  print(tfNew)
  tf
}
