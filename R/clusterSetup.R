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
    if (!identical("localhost", unique(cores))) {
      repos <- c("https://predictiveecology.r-universe.dev", getOption("repos"))

      # FireSense needed "dqrng", "SpaDES.tools", "fireSenseUtils", "PredictiveEcology/fireSenseUtils@development",

      aa <- Require::pkgDep(unique(c("qs", "RCurl", pkgsNeeded)), recursive = TRUE)
      pkgsNeeded <- unique(Require::extractPkgName(unname(unlist(aa))))

      revtunnel <- ifelse(all(cores == "localhost"), FALSE, TRUE)

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
      dir.create(dirname(logPath), recursive = TRUE, showWarnings = FALSE)


      GDALversions <- parallel::clusterEvalQ(cl, {
        .libPaths(libPath)
        return(sf::sf_extSoftVersion()["GDAL"])
      })

      stopifnot(length(unique(sf::sf_extSoftVersion()["GDAL"], GDALversions)) == 1)
      parallel::stopCluster(cl)

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
          out <- lapply(setdiff(unique(cores), "localhost"), function(ip) {
            rsync <- Sys.which("rsync")
            st1 <- system.time(system(paste0(rsync, " -av ",
                                             filenameForTransfer, " ", ip, ":",
                                             filenameForTransfer)))
          })
          out <- clusterEvalQ(cl, {
            out <- qs::qread(file = filenameForTransfer)
            out <- reproducible::.unwrap(out)
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
          out <- clusterEvalQ(cl, {
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
    } else {
      list2env(mget(unlist(objsNeeded), envir = envir), envir = .GlobalEnv)
    }
  }

  on.exit() # remove on.exit stopCluster because it ended successfully

  control
}
