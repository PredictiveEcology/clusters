## DEoptimDashboardApp(): the dashboard of the progress files DEoptimProgress() reads.
##
## shiny::testServer() runs in a worker process, not in this session. It runs later callbacks, and once
## later's input handler has fired outside R's top level it fires every millisecond for the rest of the
## session; R's socket reads and accepts then never time out (see .nodeAnswers()), and the tests that
## stop a worker or a connection on purpose would hang.

skip_if_not_installed("shiny")

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
  expect_match(out$fits, "ELF1")
  expect_match(out$fits, "ELF2")
  expect_match(out$fits, "FINISHED")
  expect_match(out$checked, "refreshes every")
  expect_true(nzchar(out$obj1))
  expect_true(nzchar(out$par2))
})
