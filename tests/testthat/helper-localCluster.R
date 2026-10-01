## A local PSOCK cluster whose workers load the same clusters as this session: they get this session's
## .libPaths(). Without it a worker can load another installed clusters (under covr the instrumented
## copy lives in a temporary library the workers do not see), which lacks the code under test.
## The function sent has the global environment: one defined here would carry the test environment,
## whose parent is the clusters namespace, and sending it would load clusters on the worker before
## its library paths are set. Stopped when the calling test ends.
localTestCluster <- function(n, env = parent.frame()) {
  cl <- parallel::makeCluster(n)
  withr::defer(parallel::stopCluster(cl), envir = env)
  setLibs <- function(libs) .libPaths(libs)
  environment(setLibs) <- globalenv()
  parallel::clusterCall(cl, setLibs, .libPaths())
  cl
}
