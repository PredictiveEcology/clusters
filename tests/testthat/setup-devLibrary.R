## Under devtools::test()/load_all() this session has the package's source, but a PSOCK worker loads whatever
## clusters is installed in .libPaths(): an older copy lacked readTransferredObjects, .deoptimRecordFields and
## .deadNodes, so 7 tests failed on workers for code that is in the source (2026-10-20). Install the source
## into a temporary library and put it first, so the workers (which are given this session's .libPaths())
## load the code under test. Under R CMD check the package is installed already and nothing is done.
if (requireNamespace("pkgload", quietly = TRUE) && pkgload::is_dev_package("clusters")) {
  lib <- withr::local_tempdir(.local_envir = teardown_env())
  src <- find.package("clusters")
  status <- system2(file.path(R.home("bin"), "R"),
                    c("CMD", "INSTALL", "--no-docs", "--no-test-load", "--no-multiarch", "-l", shQuote(lib), shQuote(src)),
                    stdout = FALSE, stderr = FALSE)
  if (status == 0L) {
    .libPaths(c(lib, .libPaths()))
    withr::defer(.libPaths(setdiff(.libPaths(), lib)), envir = teardown_env())
  }
}
