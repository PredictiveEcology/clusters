#' Minimal HT-aware PSOCK cluster planner (parallelly-only)
#'
#' @description
#' Launches a probe cluster (1 worker per host) via [parallelly::makeClusterPSOCK()]
#' using **reverse SSH tunneling** (`revtunnel = TRUE`) and **sequential** setup (on Rstudio), 
#' **parallel** otherwise,
#' configures a **per-user library** on each worker (created via `dir.create(recursive=TRUE)`),
#' installs & loads `pkgsNeeded` in that user library, measures per-host capacity via
#' [parallelly::availableCores()] and [parallelly::freeCores()], performs a
#' **hyper-threading-aware** allocation of up to `total` workers (β penalty beyond ~50% logical
#' occupancy), and optionally launches the final cluster with the computed worker counts.
#'
#' This is intentionally minimal—no `master` argument, no diagnostics or fallbacks.
#'
#' @param hosts Character vector of hostnames reachable by SSH.
#' @param total Integer total desired workers (default 100).
#' @param beta What a hyperthread adds to a core, in (0,1] (see [.workerSpeed()]); 0.75, fitted to FireSense evaluation times.
#' @param load_memory Character; `"1min"|"5min"|"15min"` window for `freeCores()` (default `"5min"`).
#' @param fraction Numeric ≥ 0; headroom scale for `freeCores()` (default 0.9).
#' @param pkgsNeeded Character vector of packages to ensure on workers
#'   (default `c("parallelly","future","foreach")`).
#' @param user_lib_template Character; per-user library path template; `%v` expands to R version
#'   (default `"~/.local/R/%v/library"`).
#' @param install_repos Character vector of CRAN-like repos tried in order
#'   (default `c("https://cloud.r-project.org","https://cran.r-project.org")`).
#' @param rshopts Character vector of SSH options passed by parallelly. Defaults to
#'   [.sshTunnelOpts()]: [.sshKeepaliveOpts()], which bounds how long the master waits on a worker that
#'   has died, and `ExitOnForwardFailure=yes`, so a reverse tunnel that cannot be set up fails. The
#'   worker then never connects back, [makeClusterPSOCK()] gives up after `connectTimeout` and retries on
#'   another port block.
#' @param rscript Character; path to `Rscript` on workers (default `"Rscript"`).
#' @param logPath Optional worker log file. A host that cannot write its folder logs to its own user
#'   cache folder instead (see `.hostLogPaths()`).
#' @param libPath Optional site library path to prepend and use if writable.
#' @param auto_stop Logical; auto-stop clusters on GC (default `TRUE`).
#' @param build_final_cluster Logical; if `TRUE` launch the final cluster (default `TRUE`).
#' @param runName Name of the fit the workers are for (the `runName` of [DEoptimIterative()]). A host gets at
#'   most as many workers as its free memory holds, at the memory per worker this fit used the last time it
#'   ran (`memMaxGB` of its latest record in `fitMemory.rds`, beside the reservations ledger). With no record
#'   for it, the largest `memMaxGB` of any fit in the window (`options(clusters.hostSpeedDays)`), and with no
#'   records at all `options(clusters.workerMemoryGB)`; when that is unset (the default) no host is capped by
#'   memory. For FireSense DEoptim workers 14 fits: peak memory was median 4.5 GB, 90th percentile about
#'   6.5 GB, maximum 14.1 GB (457 workers on 15 hosts, 2026-10-09).
#'   Every host keeps `options(clusters.memoryHeadroom)` (default 0.1) of its memory free.
#'
#' @return A list with:
#' - `probe`: per-host capacity & load,
#' - `allocation`: per-host assignments (HT-aware),
#' - `workers`: hostname vector repeated per assigned workers,
#' - `total_requested`, `total_assigned`,
#' - `cluster` (if built).
#'
#' @examples
#' \dontrun{
#' plan <- plan_psock_min(
#'   hosts = c("nodeA","nodeB","nodeC","nodeD"),
#'   total = 100,
#'   pkgsNeeded = c("parallelly","future","foreach"),
#'   user_lib_template = "~/.local/R/%v/library",
#'   build_final_cluster = TRUE
#' )
#' plan$allocation
#' parallel::stopCluster(plan$cluster)
#' }
#'
#' @references
#' parallelly PSOCK cluster setup & worker startup controls:
#'   https://parallelly.futureverse.org/reference/makeClusterPSOCK.html
#' availableCores / freeCores:
#'   https://cran.r-project.org/web/packages/parallelly/parallelly.pdf
#'   https://rdrr.io/cran/parallelly/man/freeCores.html
#' Base parallel PSOCK notes (background):
#'   https://web.mit.edu/r/current/lib/R/library/parallel/html/makeCluster.html
#' @export
plan_psock_min <- function(
  hosts,
  total = 100L,
  beta = 0.75,
  load_memory = "5min",
  fraction = 0.9,
  pkgsNeeded = c("parallelly","future","foreach"),
  user_lib_template = "~/.local/R/%v/library",
  install_repos = c("https://cloud.r-project.org","https://cran.r-project.org"),
  rshopts = .sshTunnelOpts(),
  rscript = "Rscript",
  auto_stop = TRUE,
  logPath = NULL,
  libPath = NULL,
  build_final_cluster = TRUE,
  runName = NULL
) {
  
  pkgsNeeded <- unique(c(pkgsNeeded, c("parallelly","future","foreach")))
  stopifnot(is.character(hosts), length(hosts) >= 1L)
  stopifnot(is.numeric(total), total >= 0L)
  stopifnot(is.numeric(beta), beta > 0 && beta <= 1)
  stopifnot(load_memory %in% c("1min","5min","15min"))
  
  # Propagate master .libPaths() (assumes identical FS layout across hosts)
  master_libs <- if (is.null(libPath)) .libPaths() else libPath
  
  rscript_envs <- c(R_LIBS_USER = user_lib_template)
  # This R's Rscript when every host is this machine (see .localRscript), and one OpenBLAS thread per
  # worker; the cap must prefix the command (see ?.workerRscript).
  rscript <- .workerRscript(.localRscript(rscript, hosts))

  # Single-line startup: set master libs + create per-user library, prepend it
  startup_lines <- c(
    sprintf('master_libs <- c(%s)', paste(sprintf('"%s"', master_libs), collapse = ", ")),
    'if (length(master_libs) > 0) .libPaths(c(master_libs, .libPaths()))',
    'user_lib_raw <- Sys.getenv("R_LIBS_USER")',
    'user_lib <- normalizePath(path.expand(gsub("%v", paste(R.version$major, R.version$minor, sep="."), user_lib_raw)), mustWork = FALSE)',
    'dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)',
    '.libPaths(c(user_lib, .libPaths()))'
  )
  
  ## GB per worker, looked up when asked: a fit's first chunk records it, and a running cluster
  ## deciding where its workers belong (.rebalanceFn()) should use what it has recorded since
  memPerWorker <- function() .memPerWorkerGB(runName)

  # 1) Probe cluster (one worker per host), minimal robust options
  
  ## also used, after the build, by .rebalanceFn() to probe the hosts again
  startProbe <- function(workers = hosts) makeClusterPSOCK(
    workers = workers,
    rscript = rscript,
    homogeneous = FALSE,
    rscript_libs = master_libs,
    rscript_envs = rscript_envs,
    rscript_startup = startup_lines,
    rshopts = rshopts,
    revtunnel = TRUE,             # reverse SSH tunnel (robust)  [1](https://www.hwcooling.net/en/intel-reverses-course-hyper-threading-returns-to-cpus/)
    setup_strategy = ifelse(isRstudio(), "sequential", "parallel"),# sequential avoids setup race/hangs       [1](https://www.hwcooling.net/en/intel-reverses-course-hyper-threading-returns-to-cpus/)
    autoStop = auto_stop
  )
  cl_probe <- startProbe()
  # try(): a probe node that has already gone (host OOM, dropped ssh) must not
  # turn a normal exit -- or the real error -- into "invalid connection".
  on.exit(.stopCluster(cl_probe), add = TRUE)
  
  # Export packages list (minimal, no diagnostics)
  parallel::clusterExport(cl_probe, varlist = "pkgsNeeded", envir = environment())

  ## Where each host's workers write their log (see .hostLogPaths())
  hostLogs <- .hostLogPaths(cl_probe, hosts, logPath)
  
  # 2) Install & load pkgsNeeded strictly into per-user library
 # Deal with other package stuff
  revtunnel <- FALSE
  ldPrefixed <- FALSE
  verifiedHosts <- hosts
  allLocalhost <- identical("localhost", unique(hosts))
  # `pkgsNeeded` stays as given: what the workers must be able to load. Its
  # dependency closure is not computed here any more -- the whole master
  # library is mirrored below, so nothing can be missed the way `ps` (needed by
  # Require) was on 2026-09-07 when only a computed subset was synced.
  pkgsTopLevel <- pkgsNeeded

  if (!allLocalhost) {
    revtunnel <- ifelse(allLocalhost, FALSE, TRUE)
    coresUnique <- setdiff(unique(hosts), "localhost")
    message("copying packages to: ", paste(coresUnique, collapse = ", "))
    
    # st <- system.time({
    #   cl_probe <- makeClusterPSOCK(
    #     coresUnique,
    #     # port = port_block,
    #     # revtunnel = TRUE,
    #     # rshopts = c("-o", "ExitOnForwardFailure=yes"),
    #     # tries = 5L,
    #     #delay = 5, 
    #     # renice = 20, 
    #     rscript_libs = libPath
    #   )
    #   on.exit(try(parallel::stopCluster(cl_probe), silent = TRUE), add = TRUE)
      
    #   # cl <- parallelly::makeClusterPSOCK(coresUnique, revtunnel = revtunnel, rscript_libs = libPath,
    #   #                                    renice = 20
    #   #                                    # , rscript = c("nice", RscriptPath)
    #   # )
    # })
    parallel::clusterExport(cl_probe, list("master_libs", "pkgsNeeded"),
                            envir = environment())
    
    # The master library is mirrored to every host below and other jobs may be
    # running from it right now. Installing into it here corrupts their
    # lazy-load databases and hands rsync a moving target, so anything missing
    # is reported, not installed: it belongs in project setup, before workers
    # launch. Packages found only in another library on the master (base,
    # recommended) are assumed present on hosts with the same R.
    missingPkgs <- pkgsNeeded[!.libraryHas(pkgsNeeded, unique(c(master_libs, .libPaths())))]
    if (length(missingPkgs))
      stop("Not installed on the master (", master_libs[1], "): ",
           paste(missingPkgs, collapse = ", "),
           ". Install them as part of project setup before launching workers; ",
           "this function does not install into a library that running workers share.")
    
    parallel::clusterEvalQ(cl_probe, {
      # If this is first time that packages need to be installed for this user on this machine
      #   there won't be a folder present that is writable
      if (!dir.exists(master_libs[1])) {
        dir.create(master_libs[1], recursive = TRUE)
      }
    })
    
    message("Setting up packages on the cluster...")
    out <- lapply(setdiff(coresUnique, "localhost"), function(ip) {
      rsync <- Sys.which("rsync")
      if (!nzchar(rsync))
        stop("rsync not found on PATH; it is required to sync the project library to ", ip)
      # NOTE: deliberately NOT `--update`. That flag skips files that are newer on
      # the receiver, so a downgrade on the master would never propagate and a
      # stale-but-newer remote copy would win. The master is the single source of
      # truth for what the workers run, so mirror the whole library exactly
      # (`--delete` prunes what the master no longer has). Whole, not a computed
      # subset: a subset cannot promise the invariant in the error below.
      res <- .runWithRetry(rsync, c("-a", "--delete",
                                    shQuote(paste0(master_libs[1], "/")),
                                    shQuote(paste0(ip, ":", master_libs[1], "/"))))
      if (!identical(res$status, 0L))
        stop("rsync of the project library to '", ip, "' failed (exit ", res$status,
             ") after ", res$tries, " attempt(s): ", res$log, ". ",
             "Workers there would run a different library than the master.")
      res$status
    })
    
    # Nothing is installed on the hosts either: the mirror above is complete, and
    # a host that still cannot load a package is named by verifyClusterHosts()
    # below rather than patched over the network mid-job.
    # A host can be missing a *system* library the synced packages link against
    # (libtbb.so.12 for RcppParallel, say). That needs no root: ship the file to
    # a user-writable directory and point LD_LIBRARY_PATH at it. Done before the
    # verification below, which would otherwise reject the host for a fault that
    # is one rsync away from fixed.
    if (isTRUE(getOption("clusters.shipSystemLibs", TRUE))) {
      # `hosts`, not `coresUnique`: cl_probe has one worker per element of `hosts`
      # (localhost included) and results are matched to hosts by position. With
      # the shorter vector everything shifted by one and the last host -- kodama,
      # the one missing libtbb -- was never looked at (2026-09-08).
      shipped <- shipSystemLibs(cl_probe, hosts = hosts, libs = master_libs)
      if (length(unlist(shipped$shipped))) {
        # NOT rscript_envs: parallelly implements that as
        # `Rscript -e 'Sys.setenv(...)'`, and glibc parses LD_LIBRARY_PATH once
        # at process startup -- setting it from inside R does not move the
        # loader's search path. It has to prefix the command instead.
        # `rscript` is one value for every worker, so this needs the resolved
        # directory to be the same on all of them (it is, when they share a
        # home directory layout); otherwise say so rather than set it wrongly.
        # Only the shipped directory goes into the prefix. Each host's R wrapper
        # (etc/ldpaths) appends whatever LD_LIBRARY_PATH the process started
        # with to R's own, so an env-provided value is kept, not replaced; the
        # hosts' pre-existing values need not agree and were never needed here
        # (2026-09-08: they differed because kodama's Rscript is the system R,
        # and the prefix was refused for that reason alone).
        dests <- unique(shipped$destResolved)
        if (length(dests) == 1L) {
          ldValue <- dests
          rscript <- c("env", paste0("LD_LIBRARY_PATH=", ldValue), rscript)
          ldPrefixed <- TRUE
          message("Workers will run with LD_LIBRARY_PATH=", ldValue)
        } else {
          message("Shipped system libraries, but the hosts do not agree on a ",
                  "single LD_LIBRARY_PATH (dirs: ", paste(dests, collapse = ", "),
                  "); not setting it. Those hosts will be dropped by the ",
                  "verification below.")
        }
      }
    }

    # Verify what was just synced: R version, package versions, GDAL, and -- the
    # one that used to surface only as a dead worker mid-run -- whether the
    # packages actually load on each host. The probe was launched before any
    # system library was shipped and glibc reads LD_LIBRARY_PATH only at process
    # start, so once a prefix is set the check must run on workers launched the
    # way the final ones will be; on the probe, a host such as kodama fails
    # forever however many libraries it has been given.
    cl_verify <- cl_probe
    if (isTRUE(ldPrefixed)) {
      cl_verify <- makeClusterPSOCK(
        workers = hosts, rscript = rscript, homogeneous = FALSE,
        rscript_libs = master_libs, rscript_envs = rscript_envs,
        rscript_startup = startup_lines, rshopts = rshopts, revtunnel = TRUE,
        setup_strategy = ifelse(isRstudio(), "sequential", "parallel"),
        autoStop = auto_stop)
      on.exit(.stopCluster(cl_verify), add = TRUE)
    }
    # Same alignment rule as above: one worker per element of `hosts`.
    verifiedHosts <- verifyClusterHosts(cl_verify, hosts = hosts, pkgs = pkgsTopLevel,
                                        libs = master_libs,
                                        action = getOption("clusters.onBadHost", "stop"))
    
    # parallel::stopCluster(cl_probe)
    # Sys.sleep(2) 
  }
  


  # parallel::clusterCall(
  #   cl_probe,
  #   function(pkgs, repos, user_lib_template) {
  #     user_lib <- normalizePath(path.expand(
  #       gsub("%v", paste(R.version$major, R.version$minor, sep="."), user_lib_template)
  #     ), mustWork = FALSE)
  #     dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
  #     .libPaths(c(user_lib, .libPaths()))
  #     # Install only missing in target
  #     need <- setdiff(pkgs, rownames(installed.packages(lib.loc = user_lib)))
  #     if (length(need)) {
  #       options(repos = c(CRAN = repos[1]))
  #       install.packages(need, lib = user_lib)
  #     }
  #     invisible(lapply(pkgs, require, character.only = TRUE))
  #     TRUE
  #   },
  #   pkgsNeeded, install_repos, user_lib_template
  # )
  
  
  # --- unchanged setup above (makeClusterPSOCK with revtunnel/sequential,
  #     rscript_libs/envs/startup_lines, per-user library creation, bootstrap install) ---
  
  # 3) Measure per-host capacity (KEEP SSH LABELS)
  # stats_list <- parallel::clusterApply(
  #   cl = cl_probe,
  #   x = hosts,  # pass each SSH alias to its worker
  #   fun = function(ssh_host, memory, fraction) {
  #     # Capacity measured with parallelly, but we report the SSH label
  #     maxC <- parallelly::availableCores()
  #     freeC <- parallelly::freeCores(memory = memory, fraction = fraction)
  #     list(
  #       host       = ssh_host,                         # PRESERVE SSH NAME
  #       cores_total = as.integer(maxC),
  #       free_est    = as.integer(freeC),
  #       loadavg     = attr(freeC, "loadavg")
  #     )
  #   },
  #   memory = load_memory,
  #   fraction = fraction
  # )
  
  
  ## 2) Run the capacity queries (.probeNodes()) everywhere (same code on each worker).
  ##    Repeated until the whole population is free, up to option
  ##    clusters.waitForCores seconds (default 0: no waiting). A job that has just
  ##    spent hours preparing its inputs should queue for cores, not die
  ##    (2026-09-08: the sixth concurrent build found none free), and it must not
  ##    start with a fraction of its workers either (2026-09-15: fits ran with 5, 2
  ##    and 7 of 100). See .allocateWhenAvailable().
  nodes <- NULL
  probeCapacity <- function() {
    nodes <- .probeNodes(cl_probe, hosts, load_memory, fraction)
    # Hosts that failed verification (action = "drop") must not be allocated.
    if (!allLocalhost) nodes <- nodes[nodes$host %in% verifiedHosts, , drop = FALSE]
    nodes <- .fitCapacity(nodes, total, load_memory, memPerWorkerGB = memPerWorker())
    nodes <<- nodes
    nodes
  }
  # Book the workers the moment they are allocated, not once the cluster has started: the start and the
  # check of every worker took 5 minutes (2026-10-02), and a build deciding in those minutes took the same
  # cores. Given back if this build fails; re-booked below if workers are dropped.
  resvId <- NULL
  book <- if (isTRUE(build_final_cluster) && isTRUE(getOption("clusters.useReservations", TRUE)))
    function(alloc) resvId <<- reserveCores(alloc, memPerWorkerGB = memPerWorker())
  alloc_df <- .allocateWhenAvailable(probeCapacity, total = total, beta = beta,
                                     minFraction = getOption("clusters.minWorkersFraction", 1),
                                     waitSeconds = getOption("clusters.waitForCores", 0),
                                     book = book)
  on.exit(if (!is.null(resvId)) try(releaseCores(id = resvId), silent = TRUE), add = TRUE)

  # Build workers vector using SSH aliases preserved in allocation$host
  workers <- unlist(
    mapply(function(h, k) rep(h, k), alloc_df$host, alloc_df$assign, SIMPLIFY = FALSE),
    use.names = FALSE
  )
  
  rversion <- parallel::clusterEvalQ(cl_probe, {
    as.character(getRversion())
  })
  names(rversion) <- sapply(cl_probe, function(x) x$host)
  
  Rversions <- unique(unlist(rversion))
  haveDifferentRversions <- length(Rversions) > 1
  
  if (haveDifferentRversions) {
    dtForCores <- data.table(machine = names(rversion), Rversion = rversion)
    reproducible::messageDF(dtForCores)
    stop("Please make all machines have the same R version")
  }
  
  # Stop probe cluster and continue. try(): see the on.exit above (2026-09-08,
  # a job died here with "invalid connection" after its work was done).
  .stopCluster(cl_probe)
  
  res <- list(
    probe = nodes,
    allocation = alloc_df,
    workers = workers,
    total_requested = attr(alloc_df, "total_requested"),
    total_assigned  = attr(alloc_df, "total_assigned")
  )
  
  # 5) Final cluster (unchanged; note we pass SSH aliases in `workers`)
  if (isTRUE(build_final_cluster)) {
    if (length(workers) == 0L) stop("Allocation yielded zero workers; check capacity.")
    message("Starting ", paste(paste(names(table(workers))), "x", table(workers),
                               collapse = ", "), " clusters")
    message("Starting main parallel cluster ...")
    
    ## How a worker is started, here and when one has to be replaced (see .replaceDeadNodes()).
    startNodes <- function(workers, autoStop = auto_stop) makeClusterPSOCK(
      workers = workers,                # SSH aliases
      rscript = rscript,
      homogeneous = FALSE,
      rscript_libs = master_libs,
      rscript_envs = rscript_envs,
      rscript_startup = startup_lines,
      rshopts = rshopts,
      outfile = hostLogs,
      revtunnel = TRUE,
      setup_strategy = ifelse(isRstudio(), "sequential", "parallel"),
      autoStop = autoStop
    )
    st <- system.time(cl <- startNodes(workers))
    message(
      "it took ", round(st[3], 2), "s to start ",
      paste(paste(names(table(workers))), "x", table(workers), collapse = ", "), " threads"
    )
    
    ## Never hand back a cluster with a node that cannot answer: replace it on a new port, or drop it
    ## (options(clusters.onDeadWorker = "drop")), or stop.
    checked <- .replaceDeadNodes(cl, function(host) startNodes(host, autoStop = FALSE))
    cl <- checked$cluster
    if (length(checked$dropped)) workers <- workers[-checked$dropped]
    res$workers <- workers
    res$startNodes <- startNodes
    ## What .rebalanceFn() needs to ask, later, where this cluster's workers should be: the same probe
    ## and the same rule as this build
    res$startProbe <- function() startProbe(verifiedHosts)
    res$capacity <- function(probe, own = NULL, ownId = NULL)
      .fitCapacity(.probeNodes(probe, verifiedHosts, load_memory, fraction), total, load_memory,
                   own = own, ownId = ownId, memPerWorkerGB = memPerWorker())
    res$memPerWorkerGB <- memPerWorker
    res$beta <- beta

    on.exit()
    ## Stops the cluster current at exit: a mid-run rebuild (see .restartClusterFn()) replaces it, and
    ## stopping this one then would close whatever connections had since been given its numbers
    current <- new.env(parent = emptyenv())
    attr(cl, "currentCluster") <- current
    current$cluster <- cl
    ## and releases its reservation then, not when R collects the token: free cores are capped by every
    ## worker booked (see freeCoresLessReserved()), so a stopped cluster's booking counted in full
    ## against the next build until garbage collection
    on.exitAny({
      .stopCluster(current$cluster)
      if (!is.null(resvId)) try(releaseCores(id = resvId), silent = TRUE)
    }, 3)

    # The booking made at allocation: released when this process ends -- liveReservations() drops
    # entries whose owning pid is gone -- so a killed master cannot leak, and explicitly when the
    # cluster stops (above). Workers dropped at start-up are given back.
    if (!is.null(resvId)) {
      if (length(checked$dropped)) {
        kept <- as.data.frame(table(host = workers), stringsAsFactors = FALSE)
        names(kept)[2] <- "assign"
        .rebookCores(resvId, kept, memPerWorkerGB = memPerWorker())
      }
      # Tie the release to the lifetime of the cluster object: when it is garbage
      # collected, or R exits, this reservation goes with it. liveReservations()
      # additionally drops entries whose owning process is gone, so a master that
      # is killed outright cannot leak one either.
      token <- new.env(parent = emptyenv())
      token$id <- resvId   # .rebalanceFn() re-books it when workers move
      reg.finalizer(token, function(e) try(releaseCores(id = resvId), silent = TRUE),
                    onexit = TRUE)
      attr(cl, "reservationToken") <- token
    }
    res$cluster <- cl
  }
  
  res
}

## One row per host of `probe` (one worker per host, labelled by `hosts`, the SSH aliases): its threads,
## physical cores, parallelly::freeCores() over `load_memory`, scaled by `fraction`, and the memory the
## kernel says is available and total (GB; NA where /proc/meminfo cannot be read)
.probeNodes <- function(probe, hosts, load_memory, fraction) {
  parallel::clusterExport(probe, varlist = c("load_memory", "fraction"), envir = environment())
  caps <- parallel::clusterEvalQ(probe, {
    maxC  <- parallelly::availableCores()
    freeC <- parallelly::freeCores(memory = load_memory, fraction = fraction)
    list(
      nodename = Sys.info()[["nodename"]],
      cores_total = as.integer(maxC),
      # NA if the host cannot say (or runs an older clusters): the allocator then assumes 2 threads/core
      cores_physical = tryCatch(clusters:::.availablePhysicalCores(maxC), error = function(e) NA_integer_),
      # memory modules, from the kernel's EDAC records (readable without root); 0 where it has none
      dimms       = length(Sys.glob("/sys/devices/system/edac/mc/mc*/dimm*")),
      free_est    = as.integer(freeC),
      loadavg     = attr(freeC, "loadavg"),
      mem_available_gb = tryCatch(clusters:::.procMemoryGB("MemAvailable", "/proc/meminfo"), error = function(e) NA_real_),
      mem_total_gb     = tryCatch(clusters:::.procMemoryGB("MemTotal", "/proc/meminfo"), error = function(e) NA_real_)
    )
  })
  do.call(rbind, Map(function(lbl, cap) {
    data.frame(
      host        = lbl,                         # PRESERVE SSH NAME (not Sys.info()[["nodename"]])
      nodename    = cap$nodename,                # what the evaluation records call this host
      cores_total = cap$cores_total,
      cores_physical = if (is.null(cap$cores_physical)) NA_integer_ else cap$cores_physical,
      dimms       = if (isTRUE(cap$dimms > 0)) as.integer(cap$dimms) else NA_integer_,
      free_est    = cap$free_est,
      loadavg_1   = unname(cap$loadavg["1min"]),
      loadavg_5   = unname(cap$loadavg["5min"]),
      loadavg_15  = unname(cap$loadavg["15min"]),
      mem_available_gb = if (is.null(cap$mem_available_gb)) NA_real_ else cap$mem_available_gb,
      mem_total_gb     = if (is.null(cap$mem_total_gb)) NA_real_ else cap$mem_total_gb,
      stringsAsFactors = FALSE
    )
  }, hosts, caps))
}

## The cores each host can give one cluster: the probed free cores, less what other clusters booked (see
## freeCoresLessReserved()), and `busy`, the workers already on the host: other clusters' bookings or the
## load average, whichever is larger. The rule of a new build and of a running cluster deciding where its
## workers belong (.rebalanceFn()): `own`, that cluster's workers by host, are added back to the free cores
## and taken off the load, since moving them frees what they use, and its own reservation `ownId` is not
## counted. A new build has neither. Where the workers go within these cores is .speedAllocate()'s.
##
## Memory limits the cores too, when the memory per worker is known (`memPerWorkerGB`, see
## .memPerWorkerGB()): a host keeps `getOption("clusters.memoryHeadroom", 0.1)` of its memory free, and its
## workers are at most floor((available memory - headroom) / memPerWorkerGB). Available memory is the
## probed MemAvailable, less what other builds booked and their workers have not yet grown into (see
## freeCoresLessReserved()), plus what `own` workers use. `ram_lost` is the cores memory took away.
.fitCapacity <- function(nodes, total, load_memory, own = NULL, ownId = NULL, memPerWorkerGB = NA_real_) {
  memPerWorkerGB <- if (is.null(memPerWorkerGB)) NA_real_ else as.numeric(memPerWorkerGB)[1]
  useMemory <- !is.na(memPerWorkerGB) && all(c("mem_available_gb", "mem_total_gb") %in% names(nodes))
  mine <- if (length(own)) as.numeric(own[nodes$host]) else rep(NA_real_, NROW(nodes))
  mine[is.na(mine)] <- 0
  nodes$free_est <- as.numeric(nodes$free_est) + mine
  if (useMemory) nodes$mem_free_gb <- as.numeric(nodes$mem_available_gb) + mine * memPerWorkerGB
  load <- nodes[[paste0("loadavg_", sub("min$", "", load_memory))]]
  nodes$busy <- round(pmax(if (is.null(load)) 0 else as.numeric(load) - mine, 0))
  # freeCores() reads a trailing load average, so a cluster built moments ago
  # is under-represented in it; without this, concurrent builders double-book
  # the same cores. See ?reservations.
  if (isTRUE(getOption("clusters.useReservations", TRUE))) {
    nodes <- freeCoresLessReserved(nodes, loadWindowMinutes = as.numeric(sub("min$", "", load_memory)),
                                   exclude = ownId)
    if (any(nodes$reserved > 0))
      message("Cores reserved by other live cluster builds: ",
              paste0(nodes$host[nodes$reserved > 0], "=", nodes$reserved[nodes$reserved > 0],
                     collapse = ", "))
    nodes$busy <- pmax(nodes$busy, nodes$reserved)
  }
  nodes$ram_lost <- 0
  if (useMemory) {
    headroom <- getOption("clusters.memoryHeadroom", 0.1) * as.numeric(nodes$mem_total_gb)
    byMemory <- pmax(floor((nodes$mem_free_gb - headroom) / memPerWorkerGB), 0)
    capped <- !is.na(byMemory) & byMemory < floor(nodes$free_est)   # a host that cannot say is not capped
    nodes$ram_lost[capped] <- floor(nodes$free_est[capped]) - byMemory[capped]
    nodes$free_est[capped] <- byMemory[capped]
    if (any(capped))
      message("Workers limited by free memory (", round(memPerWorkerGB, 1), " GB per worker): ",
              paste0(nodes$host[capped], "=", byMemory[capped], " (", round(nodes$mem_free_gb[capped]), " GB free)",
                     collapse = ", "))
  }
  nodes
}

#' How fast each worker on a host runs, relative to one worker alone
#'
#' A worker runs at full speed while the host's workers are no more than its memory modules (`dimms`) and its
#' physical cores. Past the modules they share memory bandwidth: speed `(dimms / n)^memExponent`. Past the
#' physical cores two workers share a core: speed `(cores_physical + beta * (n - cores_physical)) / n`. The
#' lower of the two applies. A memory-bandwidth benchmark (2026-10-01) gave `dimms / n` per thread
#' (exponent 1), but FireSense evaluations also compute: fitted to 74,031 host x generation evaluation
#' medians from 7,231 generations on 15 hosts, with the workers booked on each host (2026-10-02), the
#' exponent is 0.2 and `beta` 0.75 (hosts with 8 modules ran 1.11-1.12x the generation median at 48
#' workers, hosts with 16 ran 0.91-0.95x). Exponent 1 fit more than twice as badly.
#'
#' @param n Workers on the host, all clusters'.
#' @param dimms,physical Memory modules and physical cores; `NA` dimms means no memory limit, `NA`
#'   physical cores half of `threads`.
#' @param threads The host's threads (`cores_total`).
#' @param beta What a hyperthread adds to a core, `(0, 1]`.
#' @param memExponent How steeply speed falls past the memory modules; `getOption("clusters.memoryExponent", 0.2)`.
#' @return Speeds in `(0, 1]`, one per element of `n`.
#' @keywords internal
.workerSpeed <- function(n, dimms, physical, threads, beta,
                         memExponent = getOption("clusters.memoryExponent", 0.2)) {
  physical <- ifelse(is.na(physical), threads / 2, physical)
  mem <- ifelse(is.na(dimms) | n <= dimms, 1, (dimms / pmax(n, 1))^memExponent)
  ht <- ifelse(n <= physical, 1, (physical + beta * (n - physical)) / pmax(n, 1))
  pmin(1, mem, ht)
}

#' Place a cluster's workers where they run fastest
#'
#' A DEoptim generation lasts as long as its slowest evaluation, so a cluster is as fast as its slowest
#' worker. Workers are placed one at a time, each on the host where the workers would then run fastest
#' ([.workerSpeed()]), counting the workers already there (`busy`: other clusters' and other load; when
#' `nodes` has no `busy`, `cores_total - free_est`), and never past a host's `free_est`. A host is filled up to its memory modules and
#' physical cores before any host goes past its own; a host with few modules still gets workers, as many
#' as keep its speed level with the others'.
#'
#' @param nodes data.frame with `host`, `cores_total`, `free_est`, and optionally `busy`, `cores_physical`
#'   and `dimms`.
#' @param total Integer total workers requested.
#' @param beta What a hyperthread adds to a core, `(0, 1]` (see [.workerSpeed()]).
#' @return data.frame with `host`, `cores_total`, `free_est`, `busy` (workers already there), `assign` and
#'   `speed` (of every worker on the host once these are added), and attributes `total_requested` and
#'   `total_assigned`.
#' @keywords internal
.speedAllocate <- function(nodes, total, beta) {
  stopifnot(all(c("host", "cores_total", "free_est") %in% names(nodes)))
  C <- as.numeric(nodes$cores_total)
  free <- floor(pmax(as.numeric(nodes$free_est), 0))
  busy <- if ("busy" %in% names(nodes)) pmax(as.numeric(nodes$busy), 0) else pmax(C - pmax(as.numeric(nodes$free_est), 0), 0)
  P <- if ("cores_physical" %in% names(nodes)) as.numeric(nodes$cores_physical) else rep(NA_real_, length(C))
  D <- if ("dimms" %in% names(nodes)) as.numeric(nodes$dimms) else rep(NA_real_, length(C))
  total_needed <- min(as.integer(total), sum(free))
  assign <- integer(length(C))
  for (k in seq_len(total_needed)) {
    s <- ifelse(assign < free, .workerSpeed(busy + assign + 1, D, P, C, beta), -Inf)
    i <- which(s == max(s))
    i <- i[which.max(free[i] - assign[i])]    # a tie goes to the host with the most room left
    assign[i] <- assign[i] + 1L
  }
  out <- data.frame(host = nodes$host, cores_total = C, free_est = free, busy = busy, assign = assign,
                    speed = round(.workerSpeed(busy + assign, D, P, C, beta), 3), stringsAsFactors = FALSE)
  attr(out, "total_requested") <- total_needed
  attr(out, "total_assigned")  <- sum(out$assign)
  out
}


isRstudio <- function() {
  Sys.getenv("RSTUDIO") == 1 || .Platform$GUI == "RStudio" ||
    if (requireNamespace("rstudioapi", quietly = TRUE)) {
      rstudioapi::isAvailable()
    }
  else {
    FALSE
  }
}


on.exitAny <- function(expr, outerLevel = 2, envir = sys.frame(-abs(outerLevel)), 
                       add = TRUE, after = TRUE) {
  funExpr <- as.call(list(function() expr))
  do.call(base::on.exit, list(funExpr, add, after), envir = envir)
}
#' SSH options that let the master notice a worker has died
#'
#' PSOCK workers are reached over SSH, and the master reads results with `unserialize()` on the
#' socket. That read is bounded by parallelly's `timeout`, which we set to 30 days so that a
#' legitimately long computation is never cut off. The consequence is that if a worker's R process
#' dies *after* connecting, SSH holds the tunnel open, the socket never closes, and the master
#' blocks for up to 30 days -- in practice, forever.
#'
#' `connectTimeout` does not help: it only bounds the initial connect, which already succeeded.
#'
#' `ServerAliveInterval`/`ServerAliveCountMax` close the door from the SSH side instead. SSH probes
#' an idle connection every `ServerAliveInterval` seconds and tears it down after
#' `ServerAliveCountMax` unanswered probes, so a dead worker surfaces as a closed connection and an
#' R error within ~2 minutes rather than hanging the run. A busy worker answers the probes, so long
#' computations are unaffected -- the probes run at the SSH layer, not in R.
#'
#' Observed 2026-09-19/20: two re-scores hung for hours after a worker died at startup with
#' `Error in unserialize(node$con)` inside `workRSOCK`, and the masters had to be killed by hand.
#'
#' @param extra Character vector of additional SSH options to append.
#' @param interval Seconds between keepalive probes.
#' @param countMax Number of unanswered probes before SSH closes the connection.
#'
#' @return A character vector of SSH options for `rshopts`.
#' @export
.sshKeepaliveOpts <- function(extra = NULL, interval = 30L, countMax = 4L) {
  stopifnot(length(interval) == 1L, interval > 0, length(countMax) == 1L, countMax > 0)
  c("-T",
    "-o", "ConnectTimeout=10",
    "-o", paste0("ServerAliveInterval=", as.integer(interval)),
    "-o", paste0("ServerAliveCountMax=", as.integer(countMax)),
    extra)
}

#' SSH options for a PSOCK worker behind a reverse tunnel
#'
#' [.sshKeepaliveOpts()] plus `ForwardX11=no` and `ExitOnForwardFailure=yes`. Without the last, ssh
#' prints "Warning: remote port forwarding failed for listen port N" and stays connected with no
#' tunnel: the worker starts, cannot reach the master, and dies, and the master waits on it.
#' [plan_psock_min()] passed its own `rshopts` to [makeClusterPSOCK()], which replaced this default,
#' so its workers never carried the option (FireSense, 2026-09-29, port 28216).
#'
#' @return A character vector of SSH options for `rshopts`.
#' @export
.sshTunnelOpts <- function() {
  .sshKeepaliveOpts(c("-o", "ForwardX11=no", "-o", "ExitOnForwardFailure=yes"))
}
