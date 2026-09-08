#' Test machines
#'
#' @param NcoresMax Integer; the largest worker count to time.
#' @param N Integer; workload multiplier for the reported totals.
#' @param thinning Integer; step between the worker counts that are timed.
#' @examples
#' \dontrun{
#' ## Builds an SSH cluster and times it; needs hosts, so not run by checks.
#' # example code
#' hosts <- c("97", "106", "184", "189", "213", "217", "220")#, "102")
#' hosts <- makeHosts(ipbase = "spades", hosts)
#' hosts <- c(hosts, "132.156.148.105", "localhost")
#' cl <- parallelly::makeClusterPSOCK(hosts,
#'                                    rscript = c("nice", "/usr/local/bin/Rscript"))
#' parallel::clusterExport(cl, c("testMachine", "pkgs"))
#' parallel::clusterEvalQ(cl, {
#'   if (!require("Require")) install.packages("Require",
#'                                             repos = c("https://predictiveecology.r-universe.dev",
#'                                                       getOption("repos")))
#'   Require::Require(pkgs)
#' })
#'
#' st <- system.time(outs <- parallel::clusterApply(cl, seq_along(cl), function(x)
#'   testMachine(N = 1, NcoresMax = 10, thinning = 3)))
#'
#' names(outs) <- hosts
#' print(st)
#' getHostCombination(outs, Npops = 100)
#'
#' @importFrom data.table as.data.table
#' @importFrom parallelly makeClusterPSOCK
#' @importFrom parallel detectCores stopCluster clusterApply
#' @export
#' }
testMachine <- function(NcoresMax = parallel::detectCores(), N = 100, thinning = 5) {
  Ncores <- c(1, seq(floor(NcoresMax/thinning)) * thinning)
  coreTimes <- list(); systemTimes <- list(); estTotTime <- list(); optimisticTotTime <- list()
  cls <- parallelly::makeClusterPSOCK(NcoresMax)
  on.exit(parallel::stopCluster(cls))
  for (Ncore in Ncores) {
    message(paste0("starting with ", Ncore, " cores"))
    st <- system.time(
      out2 <- parallel::clusterApply(
        cls, 1:Ncore, fun = function(x) system.time(replicate(40, rnorm(1e5)))))
    out4 <- range(t(as.data.table(out2)[3, ]))
    names(out4) <- c("minTime", "maxTime")
    estTotTime[[Ncore]] <- out4[[2]] / Ncore * Ncore
    optimisticTotTime[[Ncore]] <- sum(sapply(out2, function(x) x[[3]])) / Ncore
    coreTimes[[Ncore]] <- out4
    systemTimes[[Ncore]] <- st[[3]]/Ncore
    message(paste0("ending with ", Ncore, " cores; time: ", format(st[[3]], digits = 3), " secs"))
  }
  list(estTotTime = estTotTime, optimisticTotTime = optimisticTotTime,
       systemTimes = systemTimes, coreTimes = coreTimes, Ncores = Ncores, N = N,
       detectedCores = parallel::detectCores())
}

#' Plot the outputs from `testMachine`
#'
#' @param Ncores Integer vector of worker counts that were timed.
#' @param coreTimes List of per-core `range()` timings, one element per entry of `Ncores`.
#' @param estTotTime List of estimated total times, one per entry of `Ncores`.
#' @param optimisticTotTime List of best-case total times, one per entry of `Ncores`.
#' @param systemTimes List of measured elapsed times, one per entry of `Ncores`.
#' @param N Integer; the workload multiplier the times are scaled to.
#' @param nam Character; host name, used in the plot title.
#' @param detectedCores Integer; what the host reports as its core count, drawn as a reference line.
#' @examples
#' \dontrun{
#' ## Needs the `outs` timings from runTests() on real hosts.
#'
#' par(mfrow = c(2,length(hosts)))
#' Map(out = outs, nam = names(outs), function(out, nam)
#'     do.call(plotMachine, append(out, list(nam = nam))))
#'
#' @export
#' }
plotMachine <- function(Ncores, coreTimes, estTotTime, optimisticTotTime, systemTimes,
                        N = 100, nam, detectedCores) {
  ncoresUsed <- "NCores used"
  N <- 100
  keep <- which(lengths(systemTimes) > 0)
  Nmult <- N/keep

  seqNcores <- keep
  slowestCore <- unlist(sapply(coreTimes, function(x) x[2])[keep])
  estTime <- unlist(estTotTime) * Nmult
  optimisticTime <- unlist(optimisticTotTime) * Nmult
  allDat <- c(unlist(systemTimes)*Nmult, estTime, optimisticTime)
  ymax <- max(allDat)
  ymin <- min(allDat)
  plot(seqNcores, unlist(systemTimes)*N, xlab = ncoresUsed,
       ylab = paste0("seconds to run ", N, " reps"), type = "l", ylim = c(ymin, ymax), main = nam)
  lines(seqNcores, optimisticTime, col = "blue", xlab = ncoresUsed)
  lines(seqNcores, estTime, col = "red", xlab = ncoresUsed)#, ylab = paste0("seconds to run ", N, " iterations; using slowest core"))
  plot(seqNcores, slowestCore, xlab = ncoresUsed, ylab = paste0("slowest core (secs)"), main = nam)
  abline(lm(slowestCore ~ seqNcores))
}

#' Pick how many workers to run on each host
#'
#' Takes the per-host timings from [runTests()] and fits, for each host, a
#' linear model of the slowest worker's time against the number of workers. The
#' models are then used to choose an allocation of `Npops` workers across the
#' hosts that finishes soonest, since a host whose per-worker time degrades
#' quickly is worth fewer workers than its core count suggests.
#'
#' @param outs A list of [testMachine()] results, one per host, named by host.
#' @param Npops Integer; total number of workers to allocate.
#'
#' @return A `data.table` with one row per host, giving the workers assigned and
#'   the predicted time.
#' @export
#' @importFrom data.table data.table setorderv rbindlist
getHostCombination <- function(outs, Npops = 100) {

  summ <- data.table(estToTime = do.call(c, lapply(outs, function(o) as.vector(unlist(o$estTotTime)))),
                     cores = do.call(c, lapply(outs, function(o) which(lengths(o$estTotTime)>0))))
  summ2 <- lapply(outs, function(o1) {
    keep <- which(lengths(o1$coreTimes) > 0)
    ll <- lapply(o1$coreTimes[keep], function(o) data.table(t(o)))
    ll <- rbindlist(ll, idcol = "cores")
    ll[, cores := keep]
  }) |> rbindlist(idcol = "host")
  detectedCores <- lapply(outs, function(o2) o2$detectedCores)
  mods <- by(summ2, summ2$host, function(x) lm(maxTime ~ cores, data = x))
  dats <- split(summ2,by = "host")
  dats <- dats[names(mods)]
  dats <- Map(d = dats, nam = names(dats), function(d, nam) {
    dc <- detectedCores[[nam]]
    data.frame(host = unique(d$host), cores = seq(1, dc))
  })

  secs <- Map(mod = mods, dat = dats,
              function(mod, dat) {
                if (coef(mod)[["cores"]] < 0) {
                  predSecs <-  coef(mod)[["(Intercept)"]]
                } else {
                  predSecs <- predict(mod, newdata = dat)
                }
                df <- data.frame(dat, predSecs = predSecs)
              })
  summ3 <- rbindlist(secs)
  summ3[]
  setorderv(summ3, cols = "predSecs")

  outAll <- summ3[, .(slowestTime = max(predSecs)), by = "host"]
  setorderv(outAll, cols = "slowestTime")
  outAll[, standardized := slowestTime/min(slowestTime)]

  onBC <- grepl("spades|^bc", outAll$host)
  if (any(onBC))
    outAll[, standardizedOnBC := slowestTime/min(slowestTime[onBC])]
  out <- summ3[1:Npops,]
  out <- out[, list(N = .N, slowestTime = max(predSecs)), by = "host"]
  out <- na.omit(out)
  list(bestCluster = out[], wholeCluster = outAll[],
       cluster = rep(out$host, out$N))
}

#' Build host names from the last octets of their addresses
#'
#' A convenience for clusters whose hosts sit on one subnet: give the final
#' octets and get back full addresses.
#'
#' @param ips Numeric or character vector of final octets, e.g. `c(11, 12)`.
#' @param ipbase Character; everything before the final octet, with its trailing dot.
#'
#' @return Character vector of addresses.
#' @export
#' @examples
#' makeHosts(c(11, 12))
makeHosts <- function(ips, ipbase = "10.20.0.") {
  paste0(ipbase, ips)
}


#' Runs test on each machine in `hosts`
#'
#' @param hosts Character vector of host names to test.
#' @param repos Repositories to install from on each host.
#' @param clustersBranch Branch of this package to install on the hosts.
#' @param RscriptPath Path to `Rscript` on the hosts.
#' @param Npops Integer; population size the timings are scaled to.
#' @export
#' @return a list; same as `getHostCombination` return.
runTests <- function(hosts, repos = c("predictiveecology.r-universe.dev", getOption("repos")),
                     clustersBranch = "main", RscriptPath = "/usr/local/bin/Rscript",
                     Npops = 100) {
  clTesting <- parallelly::makeClusterPSOCK(hosts,
                                            rscript = c("nice", RscriptPath))
  on.exit(parallel::stopCluster(clTesting))
  parallel::clusterExport(clTesting, c("clustersBranch", "repos"), envir = environment())
  parallel::clusterEvalQ(clTesting, {
    libP <- .libPaths()[1]
    if (!require("Require", lib.loc = libP)) {
      isWritable <- identical(file.info(.libPaths()[1])[["uname"]], Sys.info()[["user"]])
      if (isWritable) # does it have any packages, i.e., is it writeable
        libP <- tempfile()
      out <- install.packages("Require", repos = repos, lib = libP)
    }
    pkg <- paste0("PredictiveEcology/clusters@", clustersBranch)
    out <- try(Require::Require(pkg)) # can fail because git
  })

  st <- system.time(outs <- parallel::clusterApply(clTesting, seq_along(clTesting), function(x)
    testMachine(N = 1, NcoresMax = 10, thinning = 3)))
  names(outs) <- hosts
  print(st)
  getHostCombination(outs, Npops = Npops)
}
