## Tests never write the real per-user state: the core-reservation ledger and hostSpeed.rds live under
## tools::R_user_dir("clusters", "data") unless options(clusters.reservationsPath) is set, and a test
## that runs DEoptimIterative() or builds a cluster without setting it wrote there (2026-10-01: 347
## rows in the real hostSpeed.rds). Tests that set their own path still override this.
withr::local_options(clusters.reservationsPath = withr::local_tempdir(.local_envir = teardown_env()),
                     .local_envir = teardown_env())
