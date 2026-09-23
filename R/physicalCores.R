#' Number of physical cores on this machine
#'
#' [.ht_allocate_min()] gives a host's real cores full weight and its hyperthreads less, so it needs
#' the physical count. `parallel::detectCores(logical = FALSE)` does not give it on Linux: it returns
#' the logical count there. On Linux this reads the kernel's CPU topology instead: each logical CPU
#' lists the logical CPUs it shares a core with (`thread_siblings_list`), so the number of distinct
#' lists is the number of cores. `/proc/cpuinfo` (distinct `physical id` x `core id`) is the fallback.
#' Elsewhere (macOS, Windows) `detectCores(logical = FALSE)` is used.
#'
#' @param sysfs,cpuinfo Paths to the Linux CPU topology directory and to `cpuinfo`; arguments only so
#'   tests can point them at fixtures.
#' @return Integer number of physical cores, or `NA` if it cannot be determined.
#' @keywords internal
.physicalCores <- function(sysfs = "/sys/devices/system/cpu", cpuinfo = "/proc/cpuinfo") {
  if (identical(Sys.info()[["sysname"]], "Linux")) {
    sib <- Sys.glob(file.path(sysfs, "cpu[0-9]*", "topology", "thread_siblings_list"))
    if (length(sib)) {
      ids <- vapply(sib, function(f) readLines(f, n = 1L, warn = FALSE)[1L], character(1))
      return(length(unique(ids)))
    }
    if (file.exists(cpuinfo)) {
      x <- readLines(cpuinfo, warn = FALSE)
      field <- function(key) trimws(sub("^[^:]*:", "", grep(paste0("^", key, "\\s*:"), x, value = TRUE)))
      phys <- field("physical id"); core <- field("core id")
      if (length(core) && length(phys) == length(core))
        return(length(unique(paste(phys, core))))
    }
    return(NA_integer_)
  }
  n <- tryCatch(parallel::detectCores(logical = FALSE), error = function(e) NA_integer_)
  as.integer(n)
}

#' Physical cores among the cores this session may use
#'
#' `parallelly::availableCores()` can be fewer than the machine's logical CPUs (cgroups, affinity,
#' options). Scale the machine's physical count by the same fraction.
#'
#' @param available Cores this session may use (`parallelly::availableCores()`).
#' @param logical,physical The machine's logical and physical core counts.
#' @return Integer, or `NA` when `physical` is unknown.
#' @keywords internal
.availablePhysicalCores <- function(available, logical = parallel::detectCores(),
                                    physical = .physicalCores()) {
  if (is.na(physical) || is.na(logical) || logical < 1) return(NA_integer_)
  as.integer(min(available, round(available * physical / logical)))
}
