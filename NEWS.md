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
