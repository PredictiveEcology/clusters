## The monitor display and its memory probe. The probe is pure given a file, and
## the layout is pure given names and totals, so both are tested without a
## cluster; what needs a cluster is only the polling loop.

meminfo <- function(total = 65808400, avail = 40000000, lines = NULL) {
  f <- withr::local_tempfile(.local_envir = parent.frame())
  writeLines(lines %||% c(
    sprintf("MemTotal:       %d kB", total),
    "MemFree:         1234567 kB",
    sprintf("MemAvailable:   %d kB", avail),
    "Buffers:          123456 kB"), f)
  f
}
`%||%` <- function(a, b) if (is.null(a)) b else a

test_that(".ramFromMeminfo converts kB to GB and reports used as total minus available", {
  x <- clusters:::.ramFromMeminfo(meminfo(total = 67108864, avail = 33554432))
  expect_equal(unname(x[["total"]]), 64)
  expect_equal(unname(x[["used"]]), 32)
})

test_that(".ramFromMeminfo returns NA rather than failing on a host that has no /proc/meminfo", {
  x <- clusters:::.ramFromMeminfo(file.path(tempdir(), "no-such-meminfo"))
  expect_true(all(is.na(x)))
  expect_named(x, c("used", "total"))
})

test_that(".ramFromMeminfo returns NA when a field it needs is absent", {
  x <- clusters:::.ramFromMeminfo(meminfo(lines = c("MemTotal:  65808400 kB", "MemFree:  1 kB")))
  expect_true(all(is.na(x)))
})

test_that(".fmtRam prints one decimal, and says so when it does not know", {
  expect_equal(clusters:::.fmtRam(9.3, 64), "9.3/64.0GB")
  expect_equal(clusters:::.fmtRam(NA_real_, 64), "?/?GB")
  expect_equal(clusters:::.fmtRam(9.3, NA_real_), "?/?GB")
})

test_that("a column is as wide as the wider of the host name and its widest memory string", {
  w <- clusters:::.monitorWidths(c("biomass", "x"), c(biomass = 128, x = 1024))
  expect_equal(unname(w[1]), nchar("128.0/128.0GB"))  # memory string wins
  expect_equal(unname(w[2]), nchar("1024.0/1024.0GB"))
  wide <- clusters:::.monitorWidths("averyveryverylonghostname", c(h = 8))
  expect_equal(unname(wide), nchar("averyveryverylonghostname"))
})

test_that("rows are right-aligned in their columns and NA renders as blank", {
  row <- clusters:::.monitorRow(c("12", NA), widths = c(4L, 3L), pad = 1L)
  expect_equal(row, "  12     ")
  expect_equal(nchar(row), 4 + 1 + 3 + 1)
})

test_that(".bare re-homes a function so it carries no namespace reference", {
  f <- clusters:::.bare(clusters:::.ramFromMeminfo)
  expect_identical(environment(f), globalenv())
  expect_true(all(is.na(f(file.path(tempdir(), "nope")))))
})

test_that("monitorCluster refuses a cluster whose length does not match `cores`", {
  # Results come back one per worker and are matched to hosts by position, so a
  # mismatch would silently label every column wrongly.
  expect_error(monitorCluster(cl = list(1, 2), cores = c("a", "b", "c")),
               "matched by position")
})
