## Every function clusters calls is its own, imported in NAMESPACE, or written pkg::fun().
## FireSense settings study, 2026-09-15: run from a plain script instead of SpaDES, a fit died plotting
## DEoptim progress with "could not find function setnames". codetools then found `:=`, set, melt,
## messageDF, modifyList2, paddedFloatToChar and Plots too -- each found only because SpaDES had
## attached its package. Under SpaDES none of these fail, so only a check like this catches them.

test_that("clusters calls no function it neither defines, imports nor qualifies", {
  skip_if_not_installed("codetools")
  ns <- asNamespace("clusters")
  found <- character()
  for (f in ls(ns, all.names = TRUE)) {
    fn <- get(f, envir = ns)
    if (is.function(fn) && !is.primitive(fn))
      codetools::checkUsage(fn, name = f, all = FALSE, suppressLocalUnused = TRUE,
                            report = function(x) found <<- c(found, trimws(x)))
  }
  undefined <- grep("no visible global function definition", found, value = TRUE)
  ## "f: no visible global function definition for 'g'", with " (file.R:line)" when source references are kept
  called <- sub(".*definition for .(.+).$", "\\1", sub(" \\([^()]*:[0-9]+\\)$", "", undefined))
  ## `.()` inside data.table's `[` is data.table syntax, not a call R makes
  expect_identical(undefined[called != "."], character())
})
