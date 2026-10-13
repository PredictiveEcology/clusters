## Shared by the placement tests (test-placementCaps.R, test-reserveCapacity.R, test-waitForFullPopulation.R)

withLedger <- function(env = parent.frame())
  withr::local_options(clusters.reservationsPath = withr::local_tempdir(.local_envir = env), .local_envir = env)

## 16-thread hosts (8 cores, 8 memory modules), as the probe reports them
node <- function(host, load = 0, threads = 16L, free = floor(0.9 * (threads - load)), avail = 100, total = 128)
  data.frame(host = host, nodename = host, cores_total = threads, cores_physical = threads / 2, dimms = 8L,
             free_est = free, loadavg_5 = load, mem_available_gb = avail, mem_total_gb = total,
             stringsAsFactors = FALSE)

## A fake clock: sleep() advances now().
fakeClock <- function() {
  e <- new.env()
  e$t <- as.POSIXct("2026-09-15 12:00:00", tz = "UTC")
  e$sleeps <- 0L
  list(now = function() e$t,
       sleep = function(s) { e$t <- e$t + s; e$sleeps <- e$sleeps + 1L; invisible(NULL) },
       sleeps = function() e$sleeps)
}
