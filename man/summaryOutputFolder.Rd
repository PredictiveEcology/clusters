% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/clusterSetup.R
\name{summaryOutputFolder}
\alias{summaryOutputFolder}
\title{Summary -- wrapper around \code{dirNew} and \code{tableFiles}}
\usage{
summaryOutputFolder(
  path,
  pattern = "^.+hists/(.+)\\\\_iter.+\\\\_[[:digit:]]{6,8}.*\\\\.png"
)
}
\arguments{
\item{path}{A folder in which to search for duplicates}

\item{pattern}{The regular expression to base the \code{table} on. It should have
one and only one parenthesis i.e., \verb{(.+)}, which will be the basis of
the \code{table}. Everything outside of the \verb{(.+)} will be removed}
}
\value{
Tabulation of the files, based on the pattern.
}
\description{
Convenient wrapper.
}
