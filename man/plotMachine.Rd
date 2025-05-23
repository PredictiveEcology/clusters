% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/clusters.R
\name{plotMachine}
\alias{plotMachine}
\title{Plot the outputs from \code{testMachine}}
\usage{
plotMachine(
  Ncores,
  coreTimes,
  estTotTime,
  optimisticTotTime,
  systemTimes,
  N = 100,
  nam,
  detectedCores
)
}
\description{
Plot the outputs from \code{testMachine}
}
\examples{

par(mfrow = c(2,length(hosts)))
Map(out = outs, nam = names(outs), function(out, nam)
    do.call(plotMachine, append(out, list(nam = nam))))

}
