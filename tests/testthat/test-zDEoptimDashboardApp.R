## DEoptimDashboardApp(): the dashboard of the progress files DEoptimProgress() reads.
##
## This file is named to run last. After shiny::testServer() in a session, a read from a stopped worker
## ignores socketTimeout() (checked 2026-10-08, cause not traced): test-workerRecovery.R's SIGSTOPped worker
## blocked clusterCall() for good, not for its 2 s.

skip_if_not_installed("shiny")

test_that("DEoptimDashboardApp lists the fits and draws their plots", {
  root <- withr::local_tempdir()
  writeProgress(file.path(root, "ELF1", "DEoptimProgress_1.csv"))
  writeProgress(file.path(root, "ELF2", "DEoptimProgress_1.csv"), finished = TRUE)
  app <- DEoptimDashboardApp(root)
  expect_s3_class(app, "shiny.appobj")
  shiny::testServer(app, {
    tab <- output$fits
    expect_match(tab, "ELF1")
    expect_match(tab, "ELF2")
    expect_match(tab, "FINISHED")
    expect_match(output$checked, "refreshes every")
    expect_true(nzchar(output$obj_1$src))
    expect_true(nzchar(output$par_2$src))
  })
})
