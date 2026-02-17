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
                         cores, logPath, libPath, objsNeeded, pkgsNeeded,
                         nCoresNeeded = 100, envir = parent.frame()) {

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
      plan <- plan_psock_min(
        hosts = coresUnique,
        total = nCoresNeeded,
        
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
  
  if (!is.null(NP)) {
    control$NP <- NP
  } else {
    control$NP <- length(cores)
  }
  
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
        filenameForTransfer <- normalizePath(tempfile(fileext = ".qs2"), mustWork = FALSE, winslash = "/")
        dir.create(dirname(filenameForTransfer), recursive = TRUE, showWarnings = FALSE) # during development, this was deleted accidentally
        qs2::qs_save(objsToCopy, file = filenameForTransfer)
        stExport <- system.time({
          outExp <- parallel::clusterExport(clThird, varlist = "filenameForTransfer", envir = environment())
        })
        out11 <- parallel::clusterEvalQ(clThird, {
          therePath <- file.path("/tmp/fireSense_SpreadFit", basename(filenameForTransfer))
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
          out <- try(qs2::qs_read(file = therePath), silent = TRUE)
          if (is(out, "try-error"))
            out <- try(qs2::qs_read(file = filenameForTransfer), silent = TRUE)
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
      stMoveObjects <- system.time(parallel::clusterExport(clThird, objsNeeded, envir = environment()))
      list2env(mget(unlist(objsNeeded), envir = environment()), envir = .GlobalEnv)
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
#' @return Returns the outputs from `vmstat`
#' @export
#' @param machines Character vector of the name(s) of the PSOCK resource to
#'   query, e.g., `"n168"`
#' @param resources Column extracted in vmstat. Defaults to `"us"` or "user CPU"
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



changeNodenameToLocalhost <- function(cores) {
  sshLines <- readLines("~/.ssh/config")
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
#'
#' # This will show the active number, updated every 0.5 seconds
#' cat("Number Active CPUs right now:\n");
#' while(TRUE) {
#'   numAC <- clusters::numActiveThreads();
#'   cat("\r"); cat(numAC); cat("  ");
#'   Sys.sleep(0.5)
#' }
#'
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
#' @export
makeClusterPSOCK <- function(
    workers,
    outfile = NULL,
    rscript_libs = .libPaths(),
    ...,
    # Hardcoded defaults LAST for easy override
    port = NULL,
    rshopts = c("-T", "-o", "ConnectTimeout=10", "-o", "ForwardX11=no", "-o", "ExitOnForwardFailure=yes"),
    tries = 5L,
    delay = 5,
    renice = 20,
    revtunnel = TRUE
) {
  # workers <- rep("localhost", cores)
  if (is.null(port)) {
    global_range <- 20000:40000
    block_size <- 300
    # Random block for this master
    start <- sample(global_range, 1)
    port <- seq(start, length.out = block_size)
  }
  
  
  parallelly::makeClusterPSOCK(
    workers        = workers,
    port           = port,
    outfile        = outfile,
    rscript_libs   = rscript_libs,
    ...,
    rshopts        = rshopts,
    tries          = tries,
    delay          = delay,
    renice         = renice,
    revtunnel      = revtunnel
  )
}




