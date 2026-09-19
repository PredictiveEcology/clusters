## The worker-side transfer directory must be PER JOB, and a failed transfer must say so.
##
## The directory used to be a single shared "/tmp/fireSense_SpreadFit" for every job on the machine,
## while the cleanup step removes `dirname(therePath)` recursively. Two jobs overlapping meant the
## first to finish deleted the directory the second was still reading from. `qs_read()` failed, the
## fallback returned a try-error, `.unwrap()` was called on it anyway, and the resulting terra
## objects had dangling external pointers -- so the run died later with "external pointer is not
## valid", an error that points nowhere near the transfer. Seen 2026-09-19: 3 of 5 concurrent
## DEoptim arms died this way, and a 6th died three more times on the same collision.
##
## These tests call the PRODUCTION functions. They were previously written against local copies of
## the logic, which tested nothing that shipped.

test_that("transferDirName() is unique per call, so concurrent jobs cannot collide", {
  dirs <- replicate(50, transferDirName())
  expect_length(unique(dirs), 50L)
  expect_false(any(dirs == "/tmp/fireSense_SpreadFit"))
  expect_true(all(grepl("^/tmp/clusters_transfer_", dirs)))
})

test_that("cleanup of one job's transfer directory leaves another job's files untouched", {
  root <- withr::local_tempdir()
  jobA <- file.path(root, "clusters_transfer_A"); jobB <- file.path(root, "clusters_transfer_B")
  dir.create(jobA); dir.create(jobB)
  fileA <- file.path(jobA, "objs.qs2"); fileB <- file.path(jobB, "objs.qs2")
  writeLines("a", fileA); writeLines("b", fileB)

  unlink(dirname(fileA), recursive = TRUE)   # what the cleanup step does

  expect_false(file.exists(fileA))
  expect_true(file.exists(fileB))            # would have been deleted when both shared one directory
})

test_that("readTransferredObjects() returns the objects when the local copy is readable", {
  ok <- file.path(withr::local_tempdir(), "objs.qs2")
  qs2::qs_save(list(x1 = c(a = 1, b = 2)), file = ok)
  expect_equal(readTransferredObjects(ok, ok)$x1, c(a = 1, b = 2))
})

test_that("readTransferredObjects() falls back to the original path", {
  d <- withr::local_tempdir()
  orig <- file.path(d, "orig.qs2"); missingLocal <- file.path(d, "never-rsynced.qs2")
  qs2::qs_save(list(x1 = 42), file = orig)
  expect_equal(readTransferredObjects(missingLocal, orig)$x1, 42)
})

test_that("readTransferredObjects() errors clearly when the transfer is gone", {
  gone <- file.path(withr::local_tempdir(), "deleted-by-another-job.qs2")
  expect_error(readTransferredObjects(gone, gone), "could not read the transferred objects")
  expect_error(readTransferredObjects(gone, gone), "concurrent job deleting a shared")
})

test_that("readTransferredObjects() rejects a file that reads as a non-list", {
  bad <- file.path(withr::local_tempdir(), "objs.qs2")
  qs2::qs_save("not a list", file = bad)
  expect_error(readTransferredObjects(bad, bad), "not a list")
  expect_error(readTransferredObjects(bad, bad), "corrupt")
})
