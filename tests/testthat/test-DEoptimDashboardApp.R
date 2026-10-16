## DEoptimDashboardApp(): the dashboard of the progress files DEoptimProgress() reads.
##
## shiny::testServer() runs in a worker process, not in this session. It runs later callbacks, and once
## later's input handler has fired outside R's top level it fires every millisecond for the rest of the
## session; R's socket reads and accepts then never time out (see .nodeAnswers()), and the tests that
## stop a worker or a connection on purpose would hang.

skip_if_not_installed("shiny")
skip_if_not_installed("DT")

test_that("DEoptimDashboardApp lists the fits and draws their plots", {
  root <- withr::local_tempdir()
  writeProgress(file.path(root, "ELF1", "DEoptimProgress_1.csv"))
  writeProgress(file.path(root, "ELF2", "DEoptimProgress_1.csv"), finished = TRUE)
  expect_s3_class(DEoptimDashboardApp(root), "shiny.appobj")
  cl <- localTestCluster(1L)
  serve <- function(root) {
    res <- new.env()
    shiny::testServer(clusters::DEoptimDashboardApp(root), {
      assign("out", envir = res, list(fits = output$fits, checked = output$checked,
                                      obj1 = output$obj_1$src, par2 = output$par_2$src))
    })
    res$out
  }
  environment(serve) <- globalenv()
  out <- parallel::clusterCall(cl, serve, root)[[1]]
  expect_match(out$fits, "container")             # the rows are sent to the browser on request (server-side DT)
  expect_match(out$checked, "refreshes every")
  expect_true(nzchar(out$obj1))
  expect_true(nzchar(out$par2))
})

test_that("the top table keeps numbers and times as numbers and times, so they sort as such, also when empty", {
  root <- withr::local_tempdir()
  writeProgress(file.path(root, "ELF1", "DEoptimProgress_1.csv"))
  writeProgressErrored(file.path(root, "ELF2", "DEoptimProgress_1.csv"))
  d <- clusters:::.fitsData(DEoptimProgress(root)$fits)
  expect_identical(names(d), c("fit", "status", "generation", "best objective", "started", "updated"))
  expect_type(d$generation, "integer")
  expect_type(d$`best objective`, "double")
  expect_s3_class(d$started, "POSIXct")
  expect_s3_class(d$updated, "POSIXct")
  e <- clusters:::.fitsData(DEoptimProgress(withr::local_tempdir())$fits)
  expect_identical(nrow(e), 0L)
  expect_identical(lapply(e, class), lapply(d, class))
  expect_s3_class(clusters:::.fitsTable(DEoptimProgress(root)$fits), "datatables")
  expect_s3_class(DEoptimDashboardApp(root, which = "errored"), "shiny.appobj")
})

test_that("the cards are a CSS grid: at most 6 across, none narrower than 300 px", {
  expect_match(clusters:::.cardsCss, "display: grid", fixed = TRUE)
  expect_match(clusters:::.cardsCss, "minmax(max(300px, calc((100% - 5 * 12px) / 6)), 1fr)", fixed = TRUE)
})

test_that("the cards follow the table's displayed order, and the disclosure arrow is in the page", {
  root <- withr::local_tempdir()
  for (n in c("ELF1", "ELF2", "ELF3")) writeProgress(file.path(root, n, "DEoptimProgress_1.csv"))
  ui <- as.character(shiny::fluidPage(clusters:::.appStyle()))
  expect_match(ui, "details[open] > summary::before { content: '\\25BE", fixed = TRUE)
  expect_match(ui, "-webkit-details-marker { display: none; }", fixed = TRUE)
  cl <- localTestCluster(1L)
  serve <- function(root) {
    res <- new.env()
    shiny::testServer(clusters::DEoptimDashboardApp(root), {
      titles <- function() regmatches(output$cards$html, gregexpr("ELF[0-9]", output$cards$html))[[1]]
      first <- titles()
      session$setInputs(fits_rows_all = c(3L, 1L, 2L))
      permuted <- titles()
      session$setInputs(fits_rows_all = c(2L, 3L))       # a search that hides ELF1
      filtered <- titles()
      session$setInputs(fits_rows_all = 1:7)             # not this data: the default order
      assign("out", envir = res, list(first = first, permuted = permuted, filtered = filtered,
                                      stale = titles()))
    })
    res$out
  }
  environment(serve) <- globalenv()
  out <- parallel::clusterCall(cl, serve, root)[[1]]
  expect_identical(out$first, c("ELF1", "ELF2", "ELF3"))
  expect_identical(out$permuted, c("ELF3", "ELF1", "ELF2"))
  expect_identical(out$filtered, c("ELF2", "ELF3"))
  expect_identical(out$stale, c("ELF1", "ELF2", "ELF3"))
})
