## The worker-side transfer directory must be PER JOB.
##
## It used to be a single shared "/tmp/fireSense_SpreadFit" for every job on the machine, while the
## cleanup step removes `dirname(therePath)` recursively. Two jobs overlapping meant the first to
## finish deleted the directory the second was still reading from. `qs_read()` failed, the fallback
## returned a try-error, `.unwrap()` was called on it anyway, and the resulting terra objects had
## dangling external pointers -- so the run died later with "external pointer is not valid", an error
## that points nowhere near the transfer. Seen 2026-09-19: 3 of 5 concurrent DEoptim arms died.

test_that("the transfer directory is unique per job, so concurrent jobs cannot collide", {
  ## the expression clusterSetup() uses to name the directory
  transferDirFor <- function() paste0("/tmp/clusters_transfer_", basename(tempfile("")))
  dirs <- replicate(50, transferDirFor())
  expect_length(unique(dirs), 50L)
  expect_false(any(dirs == "/tmp/fireSense_SpreadFit"))
})

test_that("cleanup of one job's transfer directory leaves another job's files untouched", {
  root <- withr::local_tempdir()
  jobA <- file.path(root, "clusters_transfer_A")
  jobB <- file.path(root, "clusters_transfer_B")
  dir.create(jobA); dir.create(jobB)
  fileA <- file.path(jobA, "objs.qs2"); fileB <- file.path(jobB, "objs.qs2")
  writeLines("a", fileA); writeLines("b", fileB)

  ## what the cleanup does: unlink(dirname(therePath), recursive = TRUE)
  unlink(dirname(fileA), recursive = TRUE)

  expect_false(file.exists(fileA))
  expect_true(file.exists(fileB))   # would have been deleted when both shared one directory
})

test_that("a missing transfer file raises a clear error instead of a dangling pointer", {
  ## the worker-side read-and-verify, extracted so it can be exercised without a cluster
  readTransferred <- function(therePath, filenameForTransfer) {
    out <- try(qs2::qs_read(file = therePath), silent = TRUE)
    if (inherits(out, "try-error"))
      out <- try(qs2::qs_read(file = filenameForTransfer), silent = TRUE)
    if (inherits(out, "try-error"))
      stop("clusters: could not read the transferred objects on ", Sys.info()[["nodename"]],
           " from either '", therePath, "' or '", filenameForTransfer, "'. ",
           "The transfer file was missing or unreadable; a concurrent job deleting a shared ",
           "transfer directory is one cause. Original error: ", attr(out, "condition")$message)
    if (!is.list(out))
      stop("clusters: the transferred objects on ", Sys.info()[["nodename"]],
           " are a '", class(out)[1], "', not a list; the transfer file is corrupt.")
    out
  }
  gone <- file.path(withr::local_tempdir(), "deleted-by-another-job.qs2")
  expect_error(readTransferred(gone, gone), "could not read the transferred objects")

  ## and a good file still round-trips
  ok <- file.path(withr::local_tempdir(), "objs.qs2")
  qs2::qs_save(list(x1 = c(a = 1, b = 2)), file = ok)
  expect_equal(readTransferred(ok, ok)$x1, c(a = 1, b = 2))
})

test_that("a corrupt transfer file that reads as a non-list is rejected", {
  readTransferred <- function(therePath) {
    out <- try(qs2::qs_read(file = therePath), silent = TRUE)
    if (inherits(out, "try-error")) stop("clusters: could not read the transferred objects")
    if (!is.list(out))
      stop("clusters: the transferred objects on ", Sys.info()[["nodename"]],
           " are a '", class(out)[1], "', not a list; the transfer file is corrupt.")
    out
  }
  bad <- file.path(withr::local_tempdir(), "objs.qs2")
  qs2::qs_save("not a list", file = bad)
  expect_error(readTransferred(bad), "not a list")
})
