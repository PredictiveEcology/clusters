#' Setup a cluster for DEoptim
#'
#' This includes copying files over to unique(cores) machines, then loading all objects from disk in
#' each of the parallel cores.
#'
#' @param messagePrefix Character prefix for this cluster's messages, so concurrent builds can be told apart in a shared log.
#' @param itermax Integer; maximum DEoptim iterations. See [DEoptim::DEoptim.control()].
#' @param trace Logical or integer passed to `DEoptim.control()`; how often to report progress.
#' @param strategy Integer in `[1, 10]`; the DEoptim strategy variant.
#' @param initialpop Optional matrix of starting parameter sets, one row per population member.
#' @param NP Integer; number of population members. Defaults to DEoptim's own rule when `NULL`.
#' @param cores Character vector of host names, repeated once per worker wanted on that host, or a number for localhost only.
#' @param logPath Path for worker output (`outfile` of the PSOCK cluster). Each host writes its own copy.
#' @param libPath The project library. This is the master copy: it is mirrored to every host, and workers load from it.
#' @param objsNeeded Character vector naming objects in `envir` to send to the workers.
#' @param pkgsNeeded Character vector of packages the workers must be able to load. Hosts that cannot are reported by [verifyClusterHosts()].
#' @param nCoresNeeded Integer; how many workers to aim for across all hosts.
#' @param envir Environment holding `objsNeeded`; defaults to the caller's.
#' @param controlArgs Named list of further [DEoptim::DEoptim.control()] settings (for example
#'   `CR`, `F`, `c`, `p`, `reltol`), added to the returned control so they reach DEoptim. A name
#'   `DEoptim.control()` does not have is an error. `NP` is still the number of workers built, and
#'   `cluster` and `parallelType` are set here.
#' @export
#' @returns A list of items that can be passed to `DEoptim.control()`
#'
#'
clusterSetup <- function(messagePrefix = "DEoptim_",
                         itermax = 500, trace = TRUE, strategy = 3, initialpop = NULL, NP = NULL,
                         cores, logPath, libPath, objsNeeded, pkgsNeeded,
                         nCoresNeeded = 100, envir = parent.frame(), controlArgs = list()) {
  controlArgs <- .deoptimControlArgs(controlArgs)

  # if (!all(requireNamespace("qs2") && requireNamespace("reproducible") && requireNamespace("Require")))
  #   stop("Please install missing packages")
  logPath <- file.path(
    logPath,
    paste0(
      messagePrefix, format(Sys.time(), "%Y-%m-%d_%H%M%S"),
      "_pid", Sys.getpid(), ".log"
    )
  )
  dir.create(dirname(logPath), recursive = TRUE, showWarnings = FALSE)
  
  if (!is.null(cores)) {
    
    
    if (is.numeric(cores)) cores <- rep("localhost", cores)
    
    # Global range for ssh ports so there are not errors 
    #  e.g., Warning: remote port forwarding failed for listen port 11173
    # global_range <- 20000:40000
    # block_size <- 300
    # # Random block for this master
    # start <- sample(global_range, 1)
    # port_block <- seq(start, length.out = block_size)
    if (identical(sort(unique(cores)), sort(cores))) {
      # Convert local machine from its ssh name to "localhost"
      # This looks at .ssh/config, assumes that Host is used, with # comment character naming nodename
      #  e.g., "Host coco # A159576 n18 # Eliot Degradation"
      #  This should just skip and leave `cores` unchanged if that structure doesn't exist
      cores <- changeNodenameToLocalhost(cores)
      
      if (FALSE) { # this is for NRCan network; extracts names of all nodes in the .ssh/config file
        sshLines <- readLines("~/.ssh/config")
        cores <-
          gsub("^Host (.+) #.+", "\\1", grep("^Host", sshLines, value = TRUE)) |>
          gsub(pattern = "(Host )|(f$)", replacement = "", x = _) |> unique() |>
          grep(pattern = "(^(n|bc\\**|rbc)[[:digit:]])|(jump)|\\*|remote|pfc|[[:digit:]]+", invert = TRUE, value = T)
      }
      coresUnique <- unique(unlist(cores))
      # Forward what the workers must load and which library is the master.
      # Without `pkgsNeeded` the per-host verification checked only the
      # planner's three defaults, and the real packages first failed to load on
      # the final cluster with no host named. Without `libPath` the planner took
      # `.libPaths()[1]` as the master library -- an overlay or user library, not
      # the project library -- and mirrored that to the hosts instead.
      plan <- plan_psock_min(
        hosts = coresUnique,
        total = nCoresNeeded,
        pkgsNeeded = pkgsNeeded,
        libPath = libPath,
        logPath = logPath,
        build_final_cluster = TRUE
      )
      clThird <- plan$cluster
      cores <- plan$workers
      # These lines are equivalent:
      # reproducible:::on.exit2(parallel::stopCluster(clThird))
      # do.call(base::on.exit, list(stopCluster(cl), TRUE, TRUE), envir = envir)
    
      
      # clInitial <- parallelly::makeClusterPSOCK(coresUnique)
      # clInitial <- makeClusterPSOCK(
      #   coresUnique,
      #   # port = port_block,
      #   # revtunnel = TRUE,
      #   # rshopts = c("-o", "ExitOnForwardFailure=yes"),
      #   # tries = 5L,
      #   # delay = 5,
      #   # renice = 20, 
      #   rscript_libs = libPath
      # )
      
      
      # on.exit(try(parallel::stopCluster(clInitial), silent = TRUE), add = TRUE)
      # parallel::clusterExport(clInitial, varlist = "numActiveThreads")
      # names(clInitial) <- coresUnique
      # cores <- parallel::clusterEvalQ(clInitial, {
      #   ncores = parallel::detectCores()
      #   free = parallelly::freeCores()
      #   active = ncores - free
      #   canUse = ncores - active
      #   data.frame(ncores = ncores, active = active, canUse = canUse)})
      # coreState <- data.table::rbindlist(cores, idcol = "name")
      # coreState[, prop := canUse/sum(canUse)]
      # 
      # rversion <- parallel::clusterEvalQ(clInitial, {
      #   as.character(getRversion())
      # })
      # names(rversion) <- sapply(clInitial, function(x) x$host)
      # 
      # Rversions <- unique(unlist(rversion))
      # haveDifferentRversions <- length(Rversions) > 1
      # 
      # if (haveDifferentRversions) {
      #   dtForCores <- data.table(machine = names(rversion), Rversion = rversion)
      #   messageDF(dtForCores)
      #   stop("Please make all machines have the same R version")
      # }
      # 
      # # nCoresNeeded <- 100
      # if (sum(coreState$canUse) < nCoresNeeded) stop("There are too few cores to use in this cluster")
      # vec <- floor(coreState$prop * nCoresNeeded)
      # while(sum(vec) < nCoresNeeded) {
      #   wm <- which.min(vec/coreState$canUse)
      #   vec[wm] <- vec[wm] + 1
      # }
      # cores <- rep(coreState$name, vec)
      # parallel::stopCluster(clInitial)
      # Sys.sleep(2) 
    }
  }
  control <- list(itermax = itermax, trace = trace, strategy = strategy)

  if (!is.null(initialpop)) {
    control$initialpop <- initialpop
  }

  ## Every further DEoptim setting the caller gave (CR, F, c, p, reltol, ...) goes to DEoptim.
  if (is.null(NP)) NP <- controlArgs$NP
  control <- utils::modifyList(control, controlArgs[setdiff(names(controlArgs), "NP")])

  ## NP is exactly the number of workers: DEoptim evaluates one population member per worker.
  control$NP <- .clusterNP(NP, nWorkers = if (is.null(cores)) 0L else length(cores))
  
  if (!is.null(cores)) {
    message(paste0(
      "Starting parallel model fitting for ",
      messagePrefix, ". Log: ", logPath
    ))
    
    # Make sure logPath can be written in the workers -- need to create the dir
    # if (is.numeric(cores)) cores <- rep("localhost", cores)
    ## Make cluster with just one worker per machine --> don't need to do these steps
    #     multiple times per machine, if not all 'localhost'
    # revtunnel <- FALSE
    # allLocalhost <- identical("localhost", unique(cores))
    # aa <- Require::pkgDep(unique(c("qs2", "RCurl", pkgsNeeded)), recursive = TRUE)
    # pkgsNeeded <- unique(Require::extractPkgName(unname(unlist(aa))))
    # pkgsNeeded <- setdiff(pkgsNeeded, "rgdal")
    # 
    # 
    # if (!allLocalhost) {
    #   repos <- c("https://predictiveecology.r-universe.dev", getOption("repos"))
    #   revtunnel <- ifelse(allLocalhost, FALSE, TRUE)
    #   coresUnique <- setdiff(unique(cores), "localhost")
    #   message("copying packages to: ", paste(coresUnique, collapse = ", "))
    #   
    #   st <- system.time({
    #     clSecond <- makeClusterPSOCK(
    #       coresUnique,
    #       # port = port_block,
    #       # revtunnel = TRUE,
    #       # rshopts = c("-o", "ExitOnForwardFailure=yes"),
    #       # tries = 5L,
    #       #delay = 5, 
    #       # renice = 20, 
    #       rscript_libs = libPath
    #     )
    #     on.exit(try(parallel::stopCluster(clSecond), silent = TRUE), add = TRUE)
    #     
    #     # cl <- parallelly::makeClusterPSOCK(coresUnique, revtunnel = revtunnel, rscript_libs = libPath,
    #     #                                    renice = 20
    #     #                                    # , rscript = c("nice", RscriptPath)
    #     # )
    #   })
    #   parallel::clusterExport(clSecond, list("libPath", "logPath", "repos", "pkgsNeeded"),
    #                           envir = environment())
    #   
    #   # Missing `dqrng` and `sitmo`
    #   if (NROW(pkgsNeeded))
    #     Require::Install(pkgsNeeded, libPaths = libPath)
    #   
    #   parallel::clusterEvalQ(clSecond, {
    #     # If this is first time that packages need to be installed for this user on this machine
    #     #   there won't be a folder present that is writable
    #     if (!dir.exists(libPath)) {
    #       dir.create(libPath, recursive = TRUE)
    #     }
    #   })
    #   
    #   message("Setting up packages on the cluster...")
    #   out <- lapply(setdiff(unique(cores), "localhost"), function(ip) {
    #     rsync <- Sys.which("rsync")
    #     if (!nzchar(rsync)) stop()
    #     system(paste0(rsync, " -aruv --update ", paste(file.path(libPath, pkgsNeeded), collapse = " "),
    #                   " ", ip, ":", libPath))
    #   })
    #   
    #   parallel::clusterEvalQ(clSecond, {
    #     # If this is first time that packages need to be installed for this user on this machine
    #     #   there won't be a folder present that is writable
    #     if (tryCatch(packageVersion("Require") < "1.0.1.9000", error = function(e) TRUE))
    #       install.packages("Require", lib = libPath, repos = unique(c("predictiveecology.r-universe.dev", getOption("repos"))))
    #     library(Require, lib.loc = libPath)
    #     dir.create(dirname(logPath), recursive = TRUE, showWarnings = FALSE)
    #     if (NROW(pkgsNeeded))
    #       out <- Require::Install(pkgsNeeded, libPaths = libPath)
    #   })
    #   GDALversions <- parallel::clusterEvalQ(clSecond, {
    #     .libPaths(libPath)
    #     return(try(sf::sf_extSoftVersion()["GDAL"]))
    #   })
    #   stopifnot(length(unique(sf::sf_extSoftVersion()["GDAL"], GDALversions)) == 1)
    #   
    #   parallel::stopCluster(clSecond)
    #   Sys.sleep(2) 
    # }
  
    
    ## Now make full cluster with one worker per core listed in "cores"
    
    # st <- system.time({
    #   clThird <- makeClusterPSOCK(
    #     cores,
    #     # port = port_block,
    #     # revtunnel = TRUE,
    #     outfile = logPath,
    #     # rshopts = c("-o", "ExitOnForwardFailure=yes"),
    #     # tries = 5L,
    #     # delay = 5, 
    #     # renice = 20, 
    #     rscript_libs = libPath
    #   )
    #   # cl <- parallelly::makeClusterPSOCK(cores,
    #   #                                    revtunnel = revtunnel,
    #   #                                    outfile = logPath, rscript_libs = libPath,
    #   #                                    renice = 20
    #   #                                    # , rscript = c("nice", RscriptPath)
    #   # )
    # })
    
    message("loading packages in cluster nodes")
    parallel::clusterExport(clThird, "pkgsNeeded", envir = environment())
    stPackages <- system.time(parallel::clusterEvalQ(
      clThird,
      {
        for (i in pkgsNeeded) {
          library(i, character.only = TRUE)
        }
        message("loading ", i, " at ", Sys.time())
      }
    ))
    message("it took ", round(stPackages[3], 2), "s to load packages")

    ## Workers are fresh R sessions, so they are at terra's defaults regardless of what
    ## the master set: memfrac 0.5 -- half of TOTAL RAM, per worker -- memmax 16 GB and
    ## todisk FALSE. With nCoresNeeded in the hundreds that is not a policy anyone chose.
    mirrored <- mirrorTerraOptions(clThird)
    if (length(mirrored))
      message("terra options given to nodes: ",
              paste(names(mirrored), unlist(mirrored), sep = "=", collapse = " "))
    
    message("Moving objects to each node in cluster")
    stMoveObjects <- try({
      system.time({
        objsToCopy <- mget(unlist(objsNeeded), envir = envir)
        FileBackendsToCopy <- reproducible::Filenames(objsToCopy)
        hasFilename <- nzchar(FileBackendsToCopy)
        if (any(hasFilename)) {
          objsToMem <- names(FileBackendsToCopy)[hasFilename]
          objsToCopy[objsToMem] <-
            lapply(objsToCopy[objsToMem],
                   function(x) terra::toMemory(x))
        }
        objsToCopy <- reproducible::.wrap(objsToCopy)
        filenameForTransfer <- normalizePath(tempfile(fileext = ".qs2"), mustWork = FALSE, winslash = "/")
        dir.create(dirname(filenameForTransfer), recursive = TRUE, showWarnings = FALSE) # during development, this was deleted accidentally
        qs2::qs_save(objsToCopy, file = filenameForTransfer)
        stExport <- system.time({
          outExp <- parallel::clusterExport(clThird, varlist = "filenameForTransfer", envir = environment())
        })
        ## Each job gets its OWN transfer directory. This used to be a single shared
        ## "/tmp/fireSense_SpreadFit", and the cleanup below removes `dirname(therePath)`
        ## recursively -- so a second job starting while a first was still reading would have the
        ## directory deleted underneath it. `qs_read()` then failed, the `try()` fallback returned
        ## a try-error, and `.unwrap()` produced objects whose terra external pointers were invalid;
        ## the workers died with "external pointer is not valid" at the first evaluation, far from
        ## the real cause. Observed 2026-09-19: 3 of 5 concurrent DEoptim arms died this way.
        ## `basename(tempfile())` is unique per master process, so concurrent jobs no longer collide.
        transferDir <- transferDirName()
        parallel::clusterExport(clThird, varlist = "transferDir", envir = environment())
        out11 <- parallel::clusterEvalQ(clThird, {
          therePath <- file.path(transferDir, basename(filenameForTransfer))
          dir.create(dirname(therePath), recursive = TRUE, showWarnings = FALSE)
          therePath
        })
        dfThere <- data.table(nonLocalhostCores = cores, therePath = unlist(out11))
        dfThere <- unique(dfThere, on = c("nonLocalhostCores", "therePath"))
        dfThere <- dfThere[ !nonLocalhostCores %in% "localhost", ]
        # nonLocalhostCores <- setdiff(unique(cores), "localhost")
        
        if (NROW(dfThere))
          out <- Map(ip = dfThere$nonLocalhostCores, therePath = dfThere$therePath, 
                     function(ip, therePath) {
            rsync <- Sys.which("rsync")
            st1 <- system.time(system(paste0(rsync, " -av ",
                                             filenameForTransfer, " ", ip, ":",
                                             therePath)))
          })
        out <- parallel::clusterEvalQ(clThird, {
          out <- clusters::readTransferredObjects(therePath, filenameForTransfer)
          out <- reproducible::.unwrap(out, cachePath = NULL)
          list2env(out, envir = .GlobalEnv)
        })
        # Delete the file
        notDups <- !duplicated(cores)
        out <- parallel::clusterEvalQ(clThird[notDups], {
          if (dir.exists(dirname(filenameForTransfer))) {
            try(unlink(dirname(filenameForTransfer), recursive = TRUE), silent = TRUE)
          }
          if (dir.exists(dirname(therePath))) {
            try(unlink(dirname(therePath), recursive = TRUE), silent = TRUE)
          }
        })
      })
    })
    
    if (is(stMoveObjects, "try-error")) {
      message("The attempt to move objects to cluster using rsync and qs2 failed; trying clusterExport")
      stMoveObjects <- system.time(parallel::clusterExport(clThird, objsNeeded, envir = envir))
      list2env(mget(unlist(objsNeeded), envir = envir), envir = .GlobalEnv)
    }
    message("it took ", round(stMoveObjects[3], 2), "s to move objects to nodes")
    control$cluster <- clThird
  }
  if (any(cores == "localhost") || is.null(cores))
    list2env(mget(unlist(objsNeeded), envir = envir), envir = .GlobalEnv)
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
#' @param resource Character; which `ps` field to report, e.g. `"us"` for user CPU.
#' @return Returns the outputs from `vmstat`
#' @export
#' @param machines Character vector of the name(s) of the PSOCK resource to
#'   query, e.g., `"n168"`
resourcesUsed <- function(machines = "localhost", resource = "us") {
  #if (!identical("localhost", machines)) {
  cl <- suppressMessages(makeClusterPSOCK(machines))
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
#' @param secsAgo Numeric; consider files modified within this many seconds. Ignored if `after` is given.
#' @param after A time; consider files modified after it. Defaults to `secsAgo` before now.
#' @return For side effects: removed files
#' @export
#' @param path A folder in which to search for duplicates
#' @param pattern The regular expression to search for, to identify the files. This
#'   must have 1 set of parentheses (), as only the content between the () will be
#'   used for duplicate assessment, i.e., remove anything in the file that shouldn't
#'   be used.
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



changeNodenameToLocalhost <- function(cores, sshConfig = "~/.ssh/config") {
  ## no ssh config (a laptop, a CI runner): nothing to rename
  if (!file.exists(sshConfig)) return(cores)
  sshLines <- readLines(sshConfig)
  hasSelf <- grep(Sys.info()["nodename"], sshLines, value = T)
  onlyHost <- grep("^Host ", hasSelf, value = TRUE)
  whLocalhost <- gsub("^Host (\\w+).*", "\\1", onlyHost)
  if (length(whLocalhost))
    cores <- gsub(paste(whLocalhost, collapse = "|"), "localhost", cores)
  cores
}


#' Estimate the number of active threads currently being used
#'
#' This only works on non-linux operating systems, as it uses `ps`
#'
#' @param pattern An optional search pattern to look for when identifying threads
#'   that are active. If left at default, then all threads that are active will count.
#' @param minCPU The minimum CPU (in percent , i.e.,0 to 100) that a thread must
#'   be using for it to count as actively being used.
#' @export
#' @return An integer representing the current number of threads that are being used
#'   as a CPU% greater than `minCPU`. This can be used, e.g., with
#'   `parallelly::availableCores()` to estimate the number of cores that are available
#'   to be used.
#' @note
#' This does not address memory or disk use issues.
#' @examples
#' \dontrun{
#' ## Runs until interrupted, so R CMD check must not execute it.
#'
#' # This will show the active number, updated every 0.5 seconds
#' cat("Number Active CPUs right now:\n");
#' while(TRUE) {
#'   numAC <- clusters::numActiveThreads();
#'   cat("\r"); cat(numAC); cat("  ");
#'   Sys.sleep(0.5)
#' }
#'
#' }
numActiveThreads <- function (pattern = "", minCPU = 50) {
  if (!identical(.Platform$OS.type, "windows")) {
    a0 <- system("ps -ef", intern = TRUE)[-1]
    a4 <- grep(pattern, a0, value = TRUE)
    a5 <- gsub("^.*[[:digit:]]* [[:digit:]]* ([[:digit:]]{1,3}) .*$",
               "\\1", a4)
    # account for multithreading e.g., 1000% CPU use with "1 core"
    # left is sum of percents, right is sum of cores using at least minCPU
    ceiling(max(sum(as.numeric(a5)) / 100, sum(as.numeric(a5) > minCPU)))
  }
  else {
    message("Does not work on Windows")
  }
}





#' Lightweight wrapper for parallelly::makeClusterPSOCK
#'
#' @inheritParams parallelly::makeClusterPSOCK
#' @param port Optional port or port block start.
#' @param outfile Optional log file path.
#' @param rscript_libs Optional library paths for workers.
#' @param ... Additional arguments passed to parallelly::makeClusterPSOCK.
#'
#' @param rshopts Character vector of options passed to `ssh`. The defaults disable X11 forwarding and fail fast when a tunnel cannot be established.
#' @param rscript The command that starts a worker, passed to [parallelly::makeClusterPSOCK()]. It
#'   is an argument here, not left to `...`: otherwise `rscript =` partially matched `rscript_libs`
#'   and became the workers' library path whenever `rscript_libs` was not also given.
#' @param default_packages Packages R attaches in each worker; the default is parallelly's.
#' @details A worker command that starts with `env` (the OpenBLAS cap from [.workerRscript()], or an
#'   `LD_LIBRARY_PATH` prefix) is not recognised by parallelly as Rscript, so parallelly passes the
#'   default packages as an `R_DEFAULT_PACKAGES=` assignment in front of the command. With `renice`
#'   that assignment ends up after `nice`, which then fails to run it ("nice:
#'   'R_DEFAULT_PACKAGES=...': No such file or directory"). For such a command the default packages
#'   are put among `env`'s own assignments instead.
#' @export
makeClusterPSOCK <- function(
    workers,
    outfile = NULL,
    rscript_libs = .libPaths(),
    ...,
    # Hardcoded defaults LAST for easy override
    rscript = NULL,
    default_packages = c("datasets", "utils", "grDevices", "graphics", "stats", "methods"),
    port = NULL,
    rshopts = .sshKeepaliveOpts(c("-o", "ForwardX11=no", "-o", "ExitOnForwardFailure=yes")),
    tries = 5L,
    delay = 5,
    renice = 20,
    revtunnel = TRUE
) {
  # workers <- rep("localhost", cores)
  if (is.null(port)) {
    # Random block for this master, ending below the ephemeral range (see .tunnelPortBlock)
    port <- .tunnelPortBlock(workers)
  }

  if (!is.null(rscript) && identical(basename(rscript[1]), "env") && length(default_packages)) {
    ## env's assignments come first, then the program; the default packages go with them.
    isAssignment <- grepl("^[A-Za-z_][A-Za-z0-9_]*=", rscript)
    lastAssignment <- max(c(1L, which(isAssignment)))
    rscript <- append(rscript, paste0("R_DEFAULT_PACKAGES=", paste(unique(default_packages), collapse = ",")),
                      after = lastAssignment)
    default_packages <- NULL
  }

  parallelly::makeClusterPSOCK(
    workers          = workers,
    port             = port,
    outfile          = outfile,
    rscript_libs     = rscript_libs,
    rscript          = rscript,
    default_packages = default_packages,
    ...,
    rshopts          = rshopts,
    tries            = tries,
    delay            = delay,
    renice           = renice,
    revtunnel        = revtunnel
  )
}





#' Read objects transferred to a worker, failing loudly if the transfer did not arrive
#'
#' Tries the local (rsynced) copy first, then the original shared path. A failed read used to fall
#' through to [reproducible::.unwrap()] on a `try-error`, which produced objects whose terra
#' external pointers were dangling; the job then died much later with
#' `"external pointer is not valid"`, an error saying nothing about a file transfer. Verifying here
#' keeps the error where the cause is.
#'
#' @param therePath Path to the worker-local copy of the transferred objects.
#' @param filenameForTransfer Path to the original, used as a fallback.
#'
#' @return The list of transferred objects.
#' @export
readTransferredObjects <- function(therePath, filenameForTransfer) {
  out <- try(qs2::qs_read(file = therePath), silent = TRUE)
  if (inherits(out, "try-error")) {
    out <- try(qs2::qs_read(file = filenameForTransfer), silent = TRUE)
  }
  if (inherits(out, "try-error")) {
    stop("clusters: could not read the transferred objects on ", Sys.info()[["nodename"]],
         " from either '", therePath, "' or '", filenameForTransfer, "'. ",
         "The transfer file was missing or unreadable; a concurrent job deleting a shared ",
         "transfer directory is one cause. Original error: ", attr(out, "condition")$message)
  }
  if (!is.list(out)) {
    stop("clusters: the transferred objects on ", Sys.info()[["nodename"]],
         " are a '", class(out)[1], "', not a list; the transfer file is corrupt.")
  }
  out
}

#' Build the per-job worker transfer directory name
#'
#' Unique per master process. This was a single shared `/tmp/fireSense_SpreadFit` for every job,
#' while the cleanup step removes `dirname(therePath)` recursively -- so concurrent jobs deleted
#' each other's staged objects mid-read.
#'
#' @return A directory path, as a length-one character vector.
#' @export
transferDirName <- function() {
  paste0("/tmp/clusters_transfer_", basename(tempfile("")))
}
