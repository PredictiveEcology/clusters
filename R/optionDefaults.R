## Defaults of the package options, defined once. Each is read as `getOption("clusters.<name>")`, so a
## user's explicit setting (including 0) wins. These are the values FireSense production has set.
.optionDefaults <- list(
  deoptimRescoreEvery = 10L,   # re-score the DEoptim population every k generations; 0 never
  onBadHost = "drop",          # a host failing verification is dropped; "stop" kills the job
  waitForCores = 8 * 3600,     # seconds to wait for cores held by other live builds; 0 no waiting
  deoptimConvergenceWindow = 300L,   # DEoptim early stop: generations over which the median must stop improving
  deoptimMinGenerations = 350L,      # never stop early before this many generations
  deoptimConvergenceSE = 1,          # improvement allowed, in standard errors of the median
  deoptimConvergenceSDChange = 0.1   # change in spread (MAD) allowed, as a fraction of the earlier spread
)

.clustersOption <- function(name) getOption(paste0("clusters.", name), .optionDefaults[[name]])
