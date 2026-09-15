## clusterSetup() renames this machine's ssh alias to "localhost" by reading ~/.ssh/config.
## CI, 2026-09-15: on a machine with no ~/.ssh/config, clusterSetup() could not run at all --
## readLines("~/.ssh/config") stopped with "cannot open the connection".

test_that("cores are returned unchanged when there is no ssh config", {
  noConfig <- file.path(withr::local_tempdir(), "config")
  expect_identical(clusters:::changeNodenameToLocalhost(c("localhost", "birds"), sshConfig = noConfig),
                   c("localhost", "birds"))
})

test_that("the ssh alias of this machine becomes localhost", {
  config <- withr::local_tempfile()
  ## the fleet's convention: the machine's nodename in a comment on its Host line
  writeLines(c(paste0("Host selfalias # ", Sys.info()[["nodename"]]), "  HostName 10.0.0.1",
               "Host birds # another machine"), config)
  expect_identical(clusters:::changeNodenameToLocalhost(c("selfalias", "birds"), sshConfig = config),
                   c("localhost", "birds"))
})
