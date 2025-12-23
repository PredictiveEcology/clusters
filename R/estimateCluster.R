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
#' @param beta Numeric in (0,1], HT penalty beyond 50% logical occupancy (default 0.5).
#' @param load_memory Character; `"1min"|"5min"|"15min"` window for `freeCores()` (default `"5min"`).
#' @param fraction Numeric ≥ 0; headroom scale for `freeCores()` (default 0.9).
#' @param pkgsNeeded Character vector of packages to ensure on workers
#'   (default `c("parallelly","future","foreach")`).
#' @param user_lib_template Character; per-user library path template; `%v` expands to R version
#'   (default `"~/.local/R/%v/library"`).
#' @param install_repos Character vector of CRAN-like repos tried in order
#'   (default `c("https://cloud.r-project.org","https://cran.r-project.org")`).
#' @param rshopts Character vector of SSH options passed by parallelly (default `c("-T","-o","ConnectTimeout=10")`).
#' @param rscript Character; path to `Rscript` on workers (default `"Rscript"`).
#' @param logPath Optional local file to append compact bootstrap / probe info.
#' @param libPath Optional site library path to prepend and use if writable.
#' @param auto_stop Logical; auto-stop clusters on GC (default `TRUE`).
#' @param build_final_cluster Logical; if `TRUE` launch the final cluster (default `TRUE`).
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
  beta = 0.5,
  load_memory = "5min",
  fraction = 0.9,
  pkgsNeeded = c("parallelly","future","foreach"),
  user_lib_template = "~/.local/R/%v/library",
  install_repos = c("https://cloud.r-project.org","https://cran.r-project.org"),
  rshopts = c("-T","-o","ConnectTimeout=10"),
  rscript = "Rscript",
  auto_stop = TRUE,
  logPath = NULL,
  libPath = NULL,
  build_final_cluster = TRUE
) {
  
  pkgsNeeded <- unique(c(pkgsNeeded, c("parallelly","future","foreach")))
  stopifnot(is.character(hosts), length(hosts) >= 1L)
  stopifnot(is.numeric(total), total >= 0L)
  stopifnot(is.numeric(beta), beta > 0 && beta <= 1)
  stopifnot(load_memory %in% c("1min","5min","15min"))
  
  # Propagate master .libPaths() (assumes identical FS layout across hosts)
  master_libs <- if (is.null(libPath)) .libPaths() else libPath
  
  rscript_envs <- c(R_LIBS_USER = user_lib_template)
  
  # Single-line startup: set master libs + create per-user library, prepend it
  startup_lines <- c(
    sprintf('master_libs <- c(%s)', paste(sprintf('"%s"', master_libs), collapse = ", ")),
    'if (length(master_libs) > 0) .libPaths(c(master_libs, .libPaths()))',
    'user_lib_raw <- Sys.getenv("R_LIBS_USER")',
    'user_lib <- normalizePath(path.expand(gsub("%v", paste(R.version$major, R.version$minor, sep="."), user_lib_raw)), mustWork = FALSE)',
    'dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)',
    '.libPaths(c(user_lib, .libPaths()))'
  )
  
  # 1) Probe cluster (one worker per host), minimal robust options
  
  cl_probe <- makeClusterPSOCK(
    workers = hosts,
    rscript = rscript,
    homogeneous = FALSE,
    rscript_libs = master_libs,
    rscript_envs = rscript_envs,
    rscript_startup = startup_lines,
    rshopts = rshopts,
    revtunnel = TRUE,             # reverse SSH tunnel (robust)  [1](https://www.hwcooling.net/en/intel-reverses-course-hyper-threading-returns-to-cpus/)
    setup_strategy = ifelse(isRstudio(), "sequential", "parallel"),# sequential avoids setup race/hangs       [1](https://www.hwcooling.net/en/intel-reverses-course-hyper-threading-returns-to-cpus/)
    connectTimeout = 2 * 60,
    timeout = 30 * 24 * 60 * 60,
    autoStop = auto_stop
  )
  on.exit(parallel::stopCluster(cl_probe), add = TRUE)
  
  # Export packages list (minimal, no diagnostics)
  parallel::clusterExport(cl_probe, varlist = "pkgsNeeded", envir = environment())
  
  # 2) Install & load pkgsNeeded strictly into per-user library
 # Deal with other package stuff
  revtunnel <- FALSE
  allLocalhost <- identical("localhost", unique(hosts))
  aa <- Require::pkgDep(unique(c("qs", "RCurl", pkgsNeeded)), recursive = TRUE)
  pkgsNeeded <- unique(Require::extractPkgName(unname(unlist(aa))))
  pkgsNeeded <- setdiff(pkgsNeeded, "rgdal")
  
  
  if (!allLocalhost) {
    repos <- c("https://predictiveecology.r-universe.dev", getOption("repos"))
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
    parallel::clusterExport(cl_probe, list("master_libs", "logPath", "repos", "pkgsNeeded"),
                            envir = environment())
    
    # Missing `dqrng` and `sitmo`
    if (NROW(pkgsNeeded))
      Require::Install(pkgsNeeded, libPaths = master_libs)
    
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
      if (!nzchar(rsync)) stop()
      system(paste0(rsync, " -aruv --update ", paste(file.path(master_libs[1], pkgsNeeded), collapse = " "),
                    " ", ip, ":", master_libs[1]))
    })
    
    parallel::clusterEvalQ(cl_probe, {
      # If this is first time that packages need to be installed for this user on this machine
      #   there won't be a folder present that is writable
      if (tryCatch(packageVersion("Require") < "1.0.1.9000", error = function(e) TRUE))
        install.packages("Require", lib = master_libs[1], repos = unique(c("predictiveecology.r-universe.dev", getOption("repos"))))
      library(Require, lib.loc = master_libs[1])
      if (!is.null(logPath) && is.character(logPath))
        dir.create(dirname(logPath), recursive = TRUE, showWarnings = FALSE)
      if (NROW(pkgsNeeded))
        out <- Require::Install(pkgsNeeded, libPaths = master_libs)
    })
    GDALversions <- parallel::clusterEvalQ(cl_probe, {
      .libPaths(master_libs)
      return(try(sf::sf_extSoftVersion()["GDAL"]))
    })
    stopifnot(length(unique(sf::sf_extSoftVersion()["GDAL"], GDALversions)) == 1)
    
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
  
  
  ## 1) Export the parameters that the workers will use
  parallel::clusterExport(cl_probe, varlist = c("load_memory", "fraction"), envir = environment())
  
  ## 2) Run the capacity queries everywhere (same code on each worker)
  caps <- parallel::clusterEvalQ(cl_probe, {
    maxC  <- parallelly::availableCores()
    freeC <- parallelly::freeCores(memory = load_memory, fraction = fraction)
    list(
      cores_total = as.integer(maxC),
      free_est    = as.integer(freeC),
      loadavg     = attr(freeC, "loadavg")
    )
  })
  
  ## 3) Reattach SSH labels in order (hosts is your original vector of SSH aliases)
  labels <- hosts
  
  nodes <- do.call(rbind, Map(function(lbl, cap) {
    data.frame(
      host        = lbl,                         # PRESERVE SSH NAME (not Sys.info()[["nodename"]])
      cores_total = cap$cores_total,
      free_est    = cap$free_est,
      loadavg_1   = unname(cap$loadavg["1min"]),
      loadavg_5   = unname(cap$loadavg["5min"]),
      loadavg_15  = unname(cap$loadavg["15min"]),
      stringsAsFactors = FALSE
    )
  }, labels, caps))
  
  # nodes <- do.call(rbind, lapply(stats_list, function(x) {
  #   data.frame(
  #     host        = x$host,                            # SSH alias, not machine nodename
  #     cores_total = x$cores_total,
  #     free_est    = x$free_est,
  #     loadavg_1   = unname(x$loadavg["1min"]),
  #     loadavg_5   = unname(x$loadavg["5min"]),
  #     loadavg_15  = unname(x$loadavg["15min"]),
  #     stringsAsFactors = FALSE
  #   )
  # }))
  
  # 4) HT-aware allocation (unchanged)
  alloc_df <- .ht_allocate_min(nodes, total = total, beta = beta)
  
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
    messageDF(dtForCores)
    stop("Please make all machines have the same R version")
  }
  
  # Stop probe cluster and continue (unchanged)
  parallel::stopCluster(cl_probe)
  
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
    
    st <- system.time(cl <- makeClusterPSOCK(
      workers = workers,                # SSH aliases
      rscript = rscript,
      homogeneous = FALSE,
      rscript_libs = master_libs,
      rscript_envs = rscript_envs,
      rscript_startup = startup_lines,
      rshopts = rshopts,
      revtunnel = TRUE,
      setup_strategy = ifelse(isRstudio(), "sequential", "parallel"),
      connectTimeout = 2 * 60,
      timeout = 30 * 24 * 60 * 60,
      autoStop = auto_stop
    )
    )
    message(
      "it took ", round(st[3], 2), "s to start ",
      paste(paste(names(table(workers))), "x", table(workers), collapse = ", "), " threads"
    )
    
    on.exit()
    on.exitAny(stopCluster(cl), 3)
    res$cluster <- cl
  }
  
  res
}

#' Internal (minimal): HT-aware allocation heuristic
#'
#' @description
#' Minimal HT-aware allocation:
#' - `preHT_free = pmin(free_est, cores_total/2)`
#' - `HT_free    = pmax(free_est - preHT_free, 0)`
#' - `weighted   = preHT_free + beta * HT_free`
#' Proportional shares with integer rounding and caps ≤ `free_est`.
#'
#' @param nodes data.frame with `host`, `cores_total`, `free_est`.
#' @param total Integer total workers requested.
#' @param beta Numeric penalty (0,1] for HT region.
#' @return data.frame with `preHT_free`, `HT_free`, `weighted_free`, `assign`.
#' @keywords internal
.ht_allocate_min <- function(nodes, total, beta) {
  stopifnot(all(c("host","cores_total","free_est") %in% names(nodes)))
  C <- as.numeric(nodes$cores_total)
  F <- pmax(as.numeric(nodes$free_est), 0)
  
  preHT_free <- pmin(F, C/2)
  HT_free <- pmax(F - preHT_free, 0)
  weighted <- preHT_free + beta * HT_free
  
  total_needed <- min(as.integer(total), sum(F))
  alloc <- if (sum(weighted) > 0) total_needed * (weighted / sum(weighted)) else rep(0, length(F))
  alloc <- pmin(alloc, F)
  
  floor_alloc <- floor(alloc)
  remainder <- total_needed - sum(floor_alloc)
  if (remainder > 0) {
    frac <- alloc - floor_alloc
    order_idx <- order(frac, decreasing = TRUE)
    for (k in seq_len(remainder)) {
      i <- order_idx[k]
      if (floor_alloc[i] < F[i]) floor_alloc[i] <- floor_alloc[i] + 1L
    }
  }
  
  out <- data.frame(
    host = nodes$host,
    cores_total = C,
    free_est = F,
    preHT_free = preHT_free,
    HT_free = HT_free,
    weighted_free = weighted,
    assign = as.integer(floor_alloc),
    stringsAsFactors = FALSE
  )
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