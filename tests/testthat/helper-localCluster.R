## A local PSOCK cluster whose workers load the same clusters as this session: they get this session's
## .libPaths(). Without it a worker can load another installed clusters (under covr the instrumented
## copy lives in a temporary library the workers do not see), which lacks the code under test.
## The function sent has the global environment: one defined here would carry the test environment,
## whose parent is the clusters namespace, and sending it would load clusters on the worker before
## its library paths are set. Stopped when the calling test ends, by `stop`: a test whose cluster is
## stopped by what it tests needs one that leaves closed nodes alone (.stopCluster()).
localTestCluster <- function(n, env = parent.frame(), stop = parallel::stopCluster) {
  cl <- parallel::makeCluster(n)
  withr::defer(stop(cl), envir = env)
  setWorkerLibs(cl)
}

## Give the workers of `cl` this session's .libPaths() and load clusters on them; returns `cl`.
## The load is done here, not by the first call a test makes: a worker's first call that carries a clusters
## function loads the namespace, which took 5 s under covr (2 s without), and a test that gives a worker
## 2 s to answer then found a live worker dead (covr, 2026-10-09).
setWorkerLibs <- function(cl) {
  setLibs <- function(libs) { .libPaths(libs); loadNamespace("clusters"); NULL }
  environment(setLibs) <- globalenv()
  parallel::clusterCall(cl, setLibs, .libPaths())
  cl
}
