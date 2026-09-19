# clusters 0.0.48

* The worker-side transfer read and the transfer directory name are now the exported
  functions `readTransferredObjects()` and `transferDirName()`, so they are testable without a
  live multi-host cluster.

# clusters 0.0.47

* `clusterSetup()` now gives each job its own worker-side transfer directory. It previously used a
  single shared `/tmp/fireSense_SpreadFit`, which the cleanup step removes recursively, so
  concurrent jobs deleted each other's transfer file mid-read. The failure surfaced later and
  elsewhere, as terra "external pointer is not valid" on every worker.
* A failed or corrupt object transfer now errors on the worker with the path and the original
  message, instead of being passed to `.unwrap()` and producing dangling pointers.

# clusters 0.0.46

## Bug fixes

* `DEoptimIterative2()` now decides convergence from the population, not from the best value. The rule
  in 0.0.43 stopped once the best value had not improved for 200 generations. But DE never evaluates a
  surviving member again, so on a noisy objective the best value is usually a lucky draw, and each new
  lucky record restarted the count. Eight converged FireSense fits were re-scored 10 times per member:
  that rule had stopped four while their populations were still clearly improving, and run three for
  170-560 generations after their populations had flattened.

  A fit has now converged when the median of the population's values has improved by less than one
  standard error of that median (`1.2533 * sd / sqrt(n)`) over the last 200 generations, after at least
  350. Replayed over the same eight fits' recorded populations, this stops exactly the three flat ones
  (at generations 438, 693 and 444) and none of the five still improving. Fail sentinels (`1e6`) and
  missing values are left out. Settings: `clusters.deoptimConvergenceWindow` (200),
  `clusters.deoptimMinGenerations` (350), `clusters.deoptimConvergenceSE` (1).
  `clusters.deoptimNoImproveFor` is no longer used.
  
# clusters 0.0.45

## Enhancements

* `DEoptimIterative2()` can re-score the surviving population: with
  `options(clusters.deoptimRescoreEvery = k)`, every `k` generations each member is evaluated once more
  and the value DEoptim holds for it becomes the running mean of all its evaluations. DE never
  evaluates a surviving member again, so on a noisy objective a lucky low draw stays in the population
  for good and keeps beating trials that are really better. In two converged FireSense fits, re-scoring
  every member 10 times put the values DEoptim held 275-566 below the replicated means (noise SD about
  200-250), the gap grew with every generation, and in one fit DEoptim's best was only third best by
  replicated mean.

  Each re-score costs one extra evaluation per member. Members holding the `1e6` fail sentinel are not
  re-scored. `options(clusters.deoptimRescoreArgs = list(...))` overrides objective-function arguments
  for the re-scoring calls only (FireSense: `list(thresh = Inf)`, so a re-score is always a full
  evaluation); `pruneAbove` is `Inf` for them. Re-scores are cached like generations, so a stopped fit
  resumes to the same means. Off by default (`0`), and then cache keys are unchanged.

  Not changed here: the convergence rule still counts generations since `cummin(bestvalit)` last fell.
  With re-scoring a member's value can rise, so an early lucky record still anchors that count.

# clusters 0.0.44

## Enhancements

* `DEoptimIterative2()` now tells the objective function the worst value the current population would
  accept, as `pruneAbove`, so an objective function that can stop early (fireSenseUtils >= 0.2.3.9019)
  may abandon a trial whose partial score already exceeds it. This is worth doing because a
  generation is synchronous: its wall time is the slowest of its `NP` evaluations rather than the
  median, so the tail sets the clock.

  The bound is `max()` of the population's accepted values with the `1e6` fail sentinels removed --
  `max()` deliberately, not a quantile. DE compares each trial against its own parent, so a trial
  worse than the *worst* parent is worse than its own parent and would have been rejected anyway;
  the search trajectory is unchanged. A quantile would instead discard trials that beat their own
  parent, which are precisely the good-but-slow trials worth keeping. The test is on value, never on
  elapsed time.

  It is sent only to objective functions declaring `pruneAbove` or `...`, since DEoptim passes extra
  arguments straight through and any other function would fail with "unused argument". It travels in
  `dotsList`, which is in `omitArgs`, so a bound that changes every generation cannot invalidate a
  cached chunk.

# clusters 0.0.43

## Bug fixes

* `DEoptimIterative2()` decides convergence from the best value's recent history instead of from the
  p-value of a straight line through it. `bestvalit` is a monotone step function -- a FireSense fit had
  seven unique values across 456 generations, with runs of up to 141 -- so the old rule,
  `all(tail(pvals, 2) > 0.1)` over 200-generation windows, could never fire: a window spanning a step
  gives p of about 1e-44 to 1e-61, while a perfectly flat window, which is what convergence looks like,
  makes `summary.lm()` warn "essentially perfect fit" and return a degenerate p whose value is not even
  reproducible across platforms -- 0.0848 on the machine running the fits, 0.122 on the CI Linux
  runners. Whether the gate fired was therefore luck of the floating point, and on the FireSense host it
  never could: every fit ran to `itermax` (one stopped at exactly generation 1000 after 14 h).
  The new `.deoptimConverged()` stops when the best value has not improved for `noImproveFor`
  generations, after at least `minGenerations`, set by `clusters.deoptimNoImproveFor` (default 200) and
  `clusters.deoptimMinGenerations` (default 350). The window p-values are still printed as a diagnostic.
  The old rule also recomputed `pvals` only at 25-generation boundaries and reset it to `c(0, 0)`
  otherwise, so even a working test could only fire on one call in 25.

# clusters 0.0.42

## Bug fixes

* `DEoptimIterative2()` sets DEoptim's `c` to 0, with a message, when `iterStep` is more than 1. DEoptim
  (2.2.8, `src/de4_0.c`) adapts F with `meanF = (1 - c) * meanF + c * goodF2 / goodF`, where `goodF` only grows on a
  successful trial and is not reset within a call. When a call's first generation has no successful trial, `goodF`
  is 0 and `meanF` is NaN from then on, so every later trial vector is NaN. A FireSense fit with `iterStep = 5` and
  `c = 0.1` failed on all 110 workers at generation ~141 (2026-09-15). With one generation per call, the caller's
  `c` stands.

# clusters 0.0.41

## Changes

* `DEoptimIterative2()` runs `iterStep` generations in each DEoptim call, as
  `fireSenseUtils::runDEoptim()` documents; it always ran one. Each call is one cached chunk and is
  plotted when it finishes, so `iterStep` is both the restart unit and the plotting interval.
  DEoptim adapts CR and F (when `c > 0`) only within a call -- it resets them at the start of every
  call -- so one generation per call discarded that adaptation. Without `iterStep`, one generation
  per call, as before. The chunk length is part of each chunk's cache key.
* Every new objective-function evaluation's elapsed seconds are recorded where it runs and returned
  with its value as `member$evaluations` of each chunk; a message per chunk reports the minimum,
  median, 90th percentile and maximum, and the wall time per generation. The slowest evaluations set
  the time of a generation, so these show how much a long-tail cutoff could save.

# clusters 0.0.40

## Bug fixes

* `mirrorTerraOptions()`, and so `clusterSetup()`, no longer sends the cluster object to every
  worker. The function it sent was made inside it and carried `cl`, and parallelly stores each
  node's call stack (`sys.calls()`), which holds any data passed through `do.call()`. A FireSense
  fit started with `do.call(runDEoptim, args)` sent 9.6 GB to each of its 110 workers, one worker at
  a time, and stalled for hours before any objects were moved; SpaDES-run fits spent about 4.5
  minutes there (2026-09-14). Workers that received it held about 11 GB each. The function now
  has the global environment, as the probes in `monitorCluster()` already do.
* `clusterSetup()` moves the caller's objects to the workers when reproducible is not attached. It
  called `Filenames()` and `toMemory()` without their packages, which clusters does not import, and
  its fallback then looked for the objects in its own frame instead of `envir`, so it failed too
  ("object 'x1' not found"). SpaDES attaches reproducible, which hid both; a DEoptim fit run from a
  plain script stopped there (FireSense, 2026-09-15).
* `clusterSetup()` runs on a machine without `~/.ssh/config`. It reads that file to rename this
  machine's ssh alias to `localhost`, and stopped with "cannot open the connection" when it was
  missing.
* clusters works when SpaDES has not attached its dependencies. Several functions were called
  neither imported nor as `pkg::fun()`: data.table's `:=`, `set()`, `melt()` and `setnames()`,
  `messageDF()`, `modifyList2()`, `paddedFloatToChar()` and SpaDES.core's `Plots()`. A DEoptim fit run
  from a plain script died plotting its progress with "could not find function setnames" (FireSense,
  2026-09-15). A test now checks every function in the package with codetools. `DEoptimIterative2()`
  stops at the start, not at the first plot, when plots are requested and SpaDES.core (now in
  Suggests) is not installed.
* `plan_psock_min()` (and so `clusterSetup()`) starts this R's Rscript when every host is this machine.
  It started workers with a bare `Rscript`, which is whatever comes first on PATH: under
  `R CMD check --as-cran` a stub that only prints "'Rscript' should not be used without a path" and exits,
  so no worker started and the build waited for them indefinitely (this PR's Ubuntu checks); elsewhere
  possibly a different R from the master's. Clusters with remote hosts still use `Rscript` from each
  host's PATH.

# clusters 0.0.39

## Bug fixes

* `makeClusterPSOCK()` picks its port block below the ephemeral port range. parallelly gives each
  worker's reverse tunnel the ports after the master's, and the block was drawn from 20000:40000, so
  about a third of builds put tunnel ports at 32768 or above, where the kernel hands ports to outgoing
  connections on every host (32768-60999). On 2026-09-15 a FireSense worker's own connection on camas had
  been given the next tunnel's port: that tunnel could not bind, the worker could not reach the master
  ("cannot open the connection"), and the 110-worker build hung at "Starting main parallel cluster".
  The block, plus one port per worker, now ends below the start of this machine's ephemeral range
  (32768 when it cannot be read).

# clusters 0.0.36

## Bug fixes

* Workers start again when their command is prefixed with `env`, as the OpenBLAS thread cap of
  0.0.34 does. parallelly treats the first word of `rscript` as the program, so it passed the default
  packages as an `R_DEFAULT_PACKAGES=` assignment in front of the command, and `makeClusterPSOCK()`'s
  `renice = 20` then put `nice` in front of that. Every worker failed with
  "nice: 'R_DEFAULT_PACKAGES=...': No such file or directory" (FireSense, 2026-09-15). The default
  packages now go among `env`'s own assignments. The `LD_LIBRARY_PATH` prefix had the same problem.
* `makeClusterPSOCK()` has an `rscript` argument. It was left to `...`, where `rscript =` partially
  matched `rscript_libs` whenever `rscript_libs` was not also given, so the command became the
  workers' library path and plain `Rscript` was launched.

# clusters 0.0.35

## Changes

* `plan_psock_min()` (and so `clusterSetup()`) waits for the whole population it asked for
  instead of starting with whatever is free. It used to wait only when no worker at all was
  free; on the FireSense fleet (2026-09-15) fits built after others started DEoptim with 5, 2 and
  7 of their 100 workers and ran for days. Now a build re-probes every minute, with a message
  saying how many workers it could get, until the full `total` is free; at the deadline
  (`options(clusters.waitForCores)` seconds, default 0) it stops with the shortfall and the free
  cores by host. A request larger than every core on the hosts stops at once. The number of
  concurrent fits therefore limits itself to the cluster. `options(clusters.minWorkersFraction = f)`
  accepts a partial start (default 1, the whole population). Real cores are still preferred over
  hyperthreads.

# clusters 0.0.34

## Bug fixes

* Cluster workers started by `plan_psock_min()` (and so `clusterSetup()`) run with one OpenBLAS
  thread. R linked to multithreaded OpenBLAS starts one thread per logical CPU, up to 64, and
  every matrix product wakes them all. With one worker per CPU that is pure contention: on the
  FireSense fleet each DEoptim worker held 49 threads and used about 6 CPUs while the allocator
  counted one, and a 50,000 x 12 product ran 2-3x slower with the pool. The cap prefixes the
  worker command (`env OPENBLAS_NUM_THREADS=1`), because `rscript_envs` is applied after R has
  started, too late for OpenBLAS. Change it with `options(clusters.workerBlasThreads = n)`, or set it
  to `NA` to leave workers alone.

# clusters 0.0.33

## Bug fixes

* `DEoptimIterative2()` respects the caller's DEoptim settings. It merged its own defaults over
  them, so `NP` was always 10 x the number of parameters and `strategy` always 3, whatever the
  caller or the cluster said. The defaults now only fill what the caller did not set.
* `clusterSetup()` passes any further DEoptim settings through to DEoptim: its new `controlArgs`
  (for example `list(CR = 0.7, F = 0.6, c = 0.9)`) goes into the returned control. It built the
  control from `itermax`, `trace`, `strategy`, `initialpop` and `NP` only, so settings such as `c`
  never reached DEoptim. A name `DEoptim.control()` does not know is an error.
* `clusterSetup()` sets `NP` to exactly the number of workers in the cluster it built (a message
  says so when a different `NP` was requested), and stops with a clear message when fewer than 4
  workers are available, the minimum DEoptim accepts.
* `DEoptimIterative2()` no longer evaluates the carried population again in every generation. It
  runs DEoptim one generation at a time, and DEoptim evaluates its initial population before each
  generation, so every generation cost 2 x NP evaluations for NP new parameter sets. The values of
  each generation's final population are now kept (`member$popval`) and returned from a lookup,
  so a generation costs NP evaluations.
* Each generation is cached even when nested caching is turned off, as
  `spades.useCache = "eventsOnly"` does, so a stopped fit resumes from its last cached
  generation. The objective function and its arguments are digested once per run, not in every
  generation. `options(clusters.cacheDEoptimIterations = FALSE)` turns the per-generation cache off.
* `DEoptimIterative2()` calls `reproducible::Cache()` explicitly; it failed with "could not find
  function Cache" unless reproducible was attached.

# clusters 0.0.32

## Bug fixes

* `plan_psock_min()` no longer counts the cores of running clusters twice. It measures
  free cores from a trailing load average and then subtracted every live core
  reservation, although the average already carried the load of any cluster running
  for more than a few minutes. On a shared fleet, clusters built after others had
  started got a handful of workers while hosts sat idle (FireSense, 2026-09-15: 5, 2
  and 7 of 100). `freeCoresLessReserved()` now subtracts each reservation only by the
  share of its load the average has not yet absorbed,
  `workers * exp(-max(age - graceMinutes, 0) / loadWindowMinutes)`. A reservation counts
  in full for its first `graceMinutes` (2), while the new cluster's workers are still
  starting, so concurrent builds still cannot double-book.

# clusters 0.0.31

## New features

* `clusterSetup()` now gives the workers the master's terra memory settings, via the
  new `mirrorTerraOptions()`. A PSOCK worker is a fresh R session, so it started at
  terra's defaults -- `memfrac = 0.5`, `memmax = 16`, `todisk = FALSE` -- no matter what
  the master had set. `memfrac` is a fraction of the machine's *total* RAM applied per
  process, so a hundred workers each believed it could size a working buffer at half of
  total RAM, silently overriding a master that had been set to something much smaller.

# clusters 0.0.30

## New features

* `monitorCluster()` shows memory as well as threads: a second row of
  `used/totalGB` per host, and the peak of both is reported when you interrupt
  it. It returns `list(threads = , ram = )` rather than a bare thread vector.
* `ramUsageGB()` reports this machine's used and total memory, `NA` where
  \file{/proc/meminfo} does not exist.

## Notes

* Neither probe requires `clusters` on the hosts. The probe functions are
  re-homed in the global environment before being sent, so they serialise
  whole instead of as a reference to this package's namespace. The previous
  approach -- inlining the `/proc/meminfo` parser into each `clusterEvalQ()`
  call -- had three copies of it to keep in step.
* `monitorCluster()` now refuses a cluster whose worker count differs from
  `cores`. Results are matched to hosts by position, so a mismatch labelled
  every column wrongly.
* CI: `R-CMD-check` and `test-coverage` call the shared reusable workflows in
  `PredictiveEcology/actions`, rather than carrying their own copies of the
  matrix and the system-dependency install.

## Bug fixes

* `.pidAlive()` was wrong on two platforms, so core reservations were too. It
  read `/proc`, which macOS does not have, making every owner look dead there;
  and on Windows it took `tasklist` exiting 0 as proof of life, though it exits
  0 when its filter matches nothing, making every owner look alive. A dead
  owner reported alive holds cores hostage; a live owner reported dead lets a
  second builder take cores that are in use. Now `/proc` where it exists, `ps
  -p` on the other Unixes, and the pid parsed out of `tasklist` output on
  Windows -- with a test that runs on every platform.
# clusters 0.0.29

## Bug fixes

* The `LD_LIBRARY_PATH` prefix for shipped system libraries was refused whenever
  the hosts' pre-existing values differed (kodama's Rscript is the system R).
  Only the shipped directory is needed: each host's R wrapper appends the value
  the process started with to R's own paths.
* Stopping the probe cluster is wrapped in `try()`: a probe node that has gone
  away turned a normal exit into "invalid connection", masking the outcome.

## New features

* Option `clusters.waitForCores` (seconds, default 0): when other live builds
  hold every core, re-measure and wait up to that long instead of failing with
  "Allocation yielded zero workers" after hours of input preparation.

* System-library shipping and host verification were handed a host vector with
  `localhost` removed while the probe cluster has one worker per element of the
  full vector. Results are matched to hosts by position, so everything shifted
  by one and the last host was never examined: kodama's missing `libtbb.so.12`
  went unshipped, its failed loads went unreported, and it received workers
  that died on `RcppParallel`. Both steps now use the probe's own host vector,
  and this machine's own workers are no longer dropped by the allocation filter.

* `clusterSetup()` now forwards `pkgsNeeded` and `libPath` to
  `plan_psock_min()`. Without them the per-host verification checked only the
  planner's three default packages, and the planner took `.libPaths()[1]` as
  the master library -- an overlay or user library when one is first -- and
  mirrored that to the hosts instead of the project library, after which the
  hosts could not find Require in it.
* When system libraries were shipped, hosts are verified on workers launched
  with the final `LD_LIBRARY_PATH` prefix. The probe predates the shipping and
  glibc reads that variable only at process start, so verifying on the probe
  failed such a host forever. Hosts dropped by verification (option
  `clusters.onBadHost = "drop"`) no longer receive final workers.
* The whole master library is mirrored to each host, not a computed dependency
  closure. The closure missed transitive dependencies (`ps`, needed by
  Require, on 2026-09-08), and the hosts then tried to install over the
  network. Nothing is installed on the hosts any more; a host that cannot
  load a required package is named by `verifyClusterHosts()`.
* `plan_psock_min()` no longer installs into the master library. That library
  is mirrored to every host and other jobs may be running from it at that
  moment; installing into it corrupts their lazy-load databases and hands
  rsync a moving target. Anything missing is now reported with a clear
  message -- it belongs in project setup, before workers launch.
* The archived `qs` is no longer hard-coded into the worker package list.
  Nothing here or in the callers uses it, and a package absent from the master
  library was a nonexistent rsync source: every DEoptim job spent ~20 minutes
  failing to install it and then died with "rsync ... failed (exit 23)".
* rsync to a host is retried up to three times and its stderr is kept and
  reported, instead of being discarded.

# clusters 0.0.24

## New features

* `shipSystemLibs()` puts missing *system* shared libraries on a worker host
  without root. The project library is rsynced to each host, but the system
  libraries its compiled packages link against are not, so a host lacking one
  dies at load -- `libtbb.so.12: cannot open shared object file`, and with it
  RcppParallel and everything depending on it. The usual answer is
  `sudo apt-get install` on every host, which is exactly what a shared research
  cluster tends not to grant. It is not needed: a shared library is a file, so
  this detects what is actually missing (`ldd` over the synced `.so` files),
  resolves each soname on the master, copies only those into a user-writable
  directory, and reports what it could not.
  `plan_psock_min()` calls it automatically before host verification;
  `options(clusters.shipSystemLibs = FALSE)` opts out.

  Deliberately never shipped: the core runtime (`libc`, `libstdc++`, `libgcc_s`,
  `libm`, `libpthread`, `libdl`, `librt`, and the loader). Those must be the
  host's own. And nothing is shipped at all unless the host matches the master's
  architecture and libc version.

  The resulting directory is put on the workers' `LD_LIBRARY_PATH` by prefixing
  the worker command (`env LD_LIBRARY_PATH=... Rscript`), **not** via
  `parallelly`'s `rscript_envs`: that is implemented as
  `Rscript -e 'Sys.setenv(...)'`, and glibc parses `LD_LIBRARY_PATH` once at
  process startup, so setting it from inside R has no effect on later `dlopen()`.
  Verified both ways on a real host.

# clusters 0.0.23

## New features

* Core reservations. `plan_psock_min()` sized every cluster from
  `parallelly::freeCores()`, which reads a *trailing* load average (5 minutes by
  default). Two clusters built inside that window both saw the same idle cores
  and both claimed them, because the load created by the first had not yet
  reached the average the second read. Callers worked around it by staggering
  builds by more than the averaging window and hand-tuning the number of
  concurrent jobs against the size of the fleet.
  A small file-backed ledger now records what each build actually took
  (`reserveCores()`), and allocation subtracts live reservations from the
  measured free cores (`freeCoresLessReserved()`). Reservations are keyed by id,
  so one process may hold several clusters and release them independently; they
  are released when the cluster object is collected or R exits, and any entry
  whose owning process is gone is dropped on the next read, so a killed master
  cannot leak one. Disable with `options(clusters.useReservations = FALSE)`;
  point concurrent masters on different machines at one shared file with
  `options(clusters.reservationsPath = )`.

* `verifyClusterHosts()` checks, before the real cluster is built, that every
  host can actually run a worker: matching R major.minor, matching versions of
  the packages the worker must load, matching GDAL, and -- the one that
  previously surfaced only as a worker dying mid-run -- that those packages
  *load* rather than merely being present. Missing system libraries
  (`libtbb.so.12` for RcppParallel, a mismatched `libgdal.so.NN` for sf/terra)
  are caught here. All offending hosts are reported at once. Set
  `options(clusters.onBadHost = "drop")` to continue without them instead of
  erroring.

## Bug fixes

* The GDAL parity check in `plan_psock_min()` could never fire. It read
  `stopifnot(length(unique(sf::sf_extSoftVersion()["GDAL"], GDALversions)) == 1)`
  -- `unique()`'s second argument is `incomparables`, not a second vector to
  compare against, so this asked whether a length-1 vector has one unique value.
  Always true. Replaced by a real cross-host comparison in
  `verifyClusterHosts()`.

* `plan_psock_min()` no longer syncs the project library with `rsync --update`.
  That flag skips files that are *newer on the receiver*, so a downgrade on the
  master never propagated and a stale-but-newer copy on a worker won -- version
  drift by construction, on exactly the packages the run depends on. The master
  is now authoritative (`--delete` within the synced package directories), and a
  failed rsync raises rather than being ignored.
