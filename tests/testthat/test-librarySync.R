## What plan_psock_min() mirrors to the hosts and what it refuses. On
## 2026-09-07 an archived package (`qs`) hard-coded into the list was absent
## from the master library: the master tried to install it into the library
## that five running jobs shared, and rsync then failed on the nonexistent
## source with only "exit 23" to show for it.

fakeLib <- function(pkgs) {
  lib <- withr::local_tempdir(.local_envir = parent.frame())
  for (p in pkgs) {
    dir.create(file.path(lib, p))
    writeLines(paste0("Package: ", p), file.path(lib, p, "DESCRIPTION"))
  }
  lib
}

test_that(".libraryHas sees only packages with a DESCRIPTION, across several libraries", {
  a <- fakeLib(c("kSamples", "sf"))
  b <- fakeLib("data.table")
  dir.create(file.path(a, "qs"))  # a bare directory is not a package
  expect_equal(clusters:::.libraryHas(c("kSamples", "data.table", "qs", "nope"), c(a, b)),
               c(TRUE, TRUE, FALSE, FALSE))
  expect_equal(clusters:::.libraryHas(character(0), a), logical(0))
})

test_that(".runWithRetry returns on the first success and reports how many tries it took", {
  skip_on_os("windows")
  counter <- withr::local_tempfile()
  writeLines("0", counter)
  script <- withr::local_tempfile(fileext = ".sh")
  writeLines(c("#!/bin/sh", sprintf("n=$(cat %s); n=$((n+1)); echo $n > %s", counter, counter),
               "if [ $n -lt 3 ]; then echo 'rsync: link_stat failed' >&2; exit 23; fi",
               "exit 0"), script)
  Sys.chmod(script, "0755")
  res <- clusters:::.runWithRetry(script, character(0), tries = 3L, pause = 0)
  expect_equal(res$status, 0L)
  expect_equal(res$tries, 3L)
})

test_that(".runWithRetry keeps the command's stderr when every try fails", {
  skip_on_os("windows")
  script <- withr::local_tempfile(fileext = ".sh")
  writeLines(c("#!/bin/sh", "echo 'rsync: link_stat \"/lib/qs\" failed: No such file' >&2", "exit 23"), script)
  Sys.chmod(script, "0755")
  res <- clusters:::.runWithRetry(script, character(0), tries = 2L, pause = 0)
  expect_equal(res$status, 23L)
  expect_equal(res$tries, 2L)
  expect_match(res$log, "link_stat")
})


test_that("clusterSetup() forwards what the workers need and which library is the master", {
  src <- paste(deparse(clusters::clusterSetup), collapse = "\n")
  expect_match(src, "pkgsNeeded = pkgsNeeded")
  expect_match(src, "libPath = libPath")
})

test_that("shipping and verification are indexed by the probe's own host vector", {
  # cl_probe has one worker per element of `hosts`; results are matched by
  # position, so a shorter vector (coresUnique, localhost removed) shifts every
  # result and hides the last host. 2026-09-08: kodama.
  src <- paste(deparse(clusters:::plan_psock_min), collapse = "\n")
  expect_match(src, "shipSystemLibs\\(cl_probe, hosts = hosts")
  expect_match(src, "verifyClusterHosts\\(cl_verify, hosts = hosts")
  expect_false(grepl("hosts = coresUnique", src))
})
