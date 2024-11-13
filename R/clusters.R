#' Test machines
#'
#' @examples
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
#' @examples
#'
#' par(mfrow = c(2,length(hosts)))
#' Map(out = outs, nam = names(outs), function(out, nam)
#'     do.call(plotMachine, append(out, list(nam = nam))))
#'
#' @export
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
  outAll[, standardizedOnBC := slowestTime/min(slowestTime[grep("spades", host)])]
  out <- summ3[1:Npops,]
  out <- out[, list(N = .N, slowestTime = max(predSecs)), by = "host"]
  list(bestCluster = out[], wholeCluster = outAll[],
       cluster = rep(out$host, out$N))
}

#' @export
makeHosts <- function(ips, ipbase = "10.20.0.") {
  paste0(ipbase, ips)
}


#' Runs test on each machine in `hosts`
#'
#' @export
#' @return a list; same as `getHostCombination` return.
runTests <- function(hosts, repos = c("predictiveecology.r-universe.dev", getOption("repos")),
                     clustersBranch = "main", RscriptPath = "/usr/local/bin/Rscript") {
  clTesting <- parallelly::makeClusterPSOCK(hosts,
                                            rscript = c("nice", RscriptPath))
  parallel::clusterExport(clTesting, c("clustersBranch", "repos"), envir = environment())
  parallel::clusterEvalQ(clTesting, {
    if (!require("Require")) install.packages("Require", repos = repos)
    pkg <- paste0("PredictiveEcology/clusters@", clustersBranch)
    Require::Require(pkg)
  })

  st <- system.time(outs <- parallel::clusterApply(clTesting, seq_along(clTesting), function(x)
    testMachine(N = 1, NcoresMax = 10, thinning = 3)))
  names(outs) <- hosts
  print(st)
  getHostCombination(outs, Npops = 100)
}
