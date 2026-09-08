# clusters 0.0.25

## Bug fixes

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
