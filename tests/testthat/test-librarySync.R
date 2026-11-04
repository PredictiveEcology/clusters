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

test_that("probe-cluster teardown cannot mask the outcome, prefix needs one shipped dir, cores can be waited for", {
  src <- paste(deparse(clusters:::plan_psock_min), collapse = "\n")
  ## .stopCluster() stops inside try()
  expect_equal(lengths(regmatches(src, gregexpr("\\.stopCluster\\(cl_probe\\)", src))), 2L)
  expect_false(grepl("length\\(existing\\) <= 1L", src))
  expect_match(src, "clustersOption\\(\"waitForCores\"\\)")
})

test_that("every user library is mirrored to a remote host, in order; R's own is not", {
  dev <- withr::local_tempdir(); stable <- withr::local_tempdir()
  calls <- list()
  fake <- function(cmd, args, ...) { calls[[length(calls) + 1L]] <<- args; list(status = 0L, tries = 1L, log = "") }
  libs <- clusters:::.shippableLibs(c(dev, stable, .Library))
  expect_equal(libs, c(dev, stable))
  clusters:::.syncLibraries(libs, "h1", "rsync", run = fake)
  expect_length(calls, 2L)
  expect_equal(calls[[1]], c("-a", "--delete", shQuote(paste0(dev, "/")), shQuote(paste0("h1:", dev, "/"))))
  expect_equal(calls[[2]], c("-a", "--delete", shQuote(paste0(stable, "/")), shQuote(paste0("h1:", stable, "/"))))
})

test_that("a single libPath gives the one rsync call it always did", {
  one <- withr::local_tempdir()
  calls <- list()
  fake <- function(cmd, args, ...) { calls[[length(calls) + 1L]] <<- args; list(status = 0L, tries = 1L, log = "") }
  clusters:::.syncLibraries(clusters:::.shippableLibs(one), "h1", "rsync", run = fake)
  expect_length(calls, 1L)
  expect_equal(calls[[1]], c("-a", "--delete", shQuote(paste0(one, "/")), shQuote(paste0("h1:", one, "/"))))
})

test_that("a failed rsync of the second library stops with its name", {
  a <- withr::local_tempdir(); b <- withr::local_tempdir()
  fake <- function(cmd, args, ...) list(status = if (grepl(b, args[3], fixed = TRUE)) 23L else 0L, tries = 3L, log = "boom")
  expect_error(clusters:::.syncLibraries(c(a, b), "h1", "rsync", run = fake), b, fixed = TRUE)
})

test_that("plan_psock_min() creates and mirrors every shippable library", {
  src <- paste(deparse(clusters:::plan_psock_min), collapse = "\n")
  expect_match(src, "shipLibs <- \\.shippableLibs\\(master_libs\\)")
  expect_match(src, "\\.syncLibraries\\(shipLibs")
  expect_false(grepl("dir.exists\\(master_libs\\[1\\]", src))
})
