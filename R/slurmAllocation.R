## Inside a Slurm job the machine is shared: the job has its CPUs and its memory, and the node's thread
## count, load average and free memory are the node's, other users' jobs included (2026-10-28: a job of 41
## CPUs and 200 GB on a 64-CPU, 490 GB node probed the node). .probeHost() measures the host it runs on, so
## it asks here what the job it is in was given (nothing outside Slurm), and .limitToJob() applies it.

## The job's cgroup v2 memory: the nearest cgroup above this process's (Slurm puts the limit on the job's
## cgroup, not on its step's) that has a numeric memory.max. Returns the limit and the GB in use, less the
## page cache the kernel can drop, as MemAvailable is; NULL where there is none.
.cgroupMemory <- function(procCgroup, cgroupRoot) {
  line <- tryCatch(grep("^0::", readLines(procCgroup, warn = FALSE), value = TRUE), error = function(e) character(0),
                   warning = function(w) character(0))
  if (!length(line)) return(NULL)
  dir <- file.path(cgroupRoot, sub("^0::/?", "", line[1]))
  readGB <- function(file) suppressWarnings(as.numeric(tryCatch(readLines(file, n = 1L, warn = FALSE),
                                                                 error = function(e) NA_character_,
                                                                 warning = function(w) NA_character_))) / 1024^3
  while (nchar(dir) > nchar(cgroupRoot)) {
    limit <- readGB(file.path(dir, "memory.max"))     # "max" is NA: no limit at this level
    if (length(limit) && !is.na(limit)) {
      stat <- tryCatch(readLines(file.path(dir, "memory.stat"), warn = FALSE), error = function(e) character(0))
      cache <- as.numeric(sub("^inactive_file ", "", grep("^inactive_file ", stat, value = TRUE))) / 1024^3
      used <- readGB(file.path(dir, "memory.current")) - if (length(cache)) cache[1] else 0
      return(list(limitGB = limit, usedGB = if (is.na(used)) 0 else max(used, 0)))
    }
    dir <- dirname(dir)
  }
  NULL
}

## What the Slurm job this process is in was given for memory: `memLimitGB` (the job
## cgroup's memory.max, else SLURM_MEM_PER_NODE, else SLURM_MEM_PER_CPU x its CPUs) and `memAvailableGB` (that,
## less what the job already uses, when its cgroup says). NA where unknown. NULL outside a job.
.slurmAllocation <- function(procCgroup = "/proc/self/cgroup", cgroupRoot = "/sys/fs/cgroup") {
  if (!nzchar(Sys.getenv("SLURM_JOB_ID"))) return(NULL)
  num <- function(x) suppressWarnings(as.numeric(Sys.getenv(x, NA_character_)))
  cpus <- parallelly::availableCores()   # reads SLURM_CPUS_*, the cgroup's cpuset and quota
  cg <- .cgroupMemory(procCgroup, cgroupRoot)
  slurmMem <- c(num("SLURM_MEM_PER_NODE"), num("SLURM_MEM_PER_CPU") * cpus) / 1024   # MB; 0 asks for all
  slurmMem <- slurmMem[!is.na(slurmMem) & slurmMem > 0]
  limit <- if (!is.null(cg)) cg$limitGB else if (length(slurmMem)) slurmMem[1] else NA_real_
  list(memLimitGB = limit,
       memAvailableGB = if (is.null(cg)) limit else max(limit - cg$usedGB, 0))
}

## The lowest of the values that are known; NA when none is
.minKnown <- function(...) {
  x <- c(...)
  if (all(is.na(x))) NA_real_ else min(x, na.rm = TRUE)
}

## A host's probe (`cap`, as .probeHost() builds it) limited to the job it runs in (`job`, from
## .slurmAllocation()). Memory is the lower of the node's and the job's. Cores: a job given fewer CPUs
## (`cores_total`) than the node has (`systemCores`) shares it, so its CPUs are all free (the cgroup keeps others off
## them; the node's load average is other jobs' and says nothing of them), and the node's load is not
## carried into `busy` (see .fitCapacity()). What the job's own clusters have booked is in the reservations
## ledger, which counts it (see freeCoresLessReserved()). A job given every CPU is the node's: probed as before.
.limitToJob <- function(cap, job, systemCores) {
  if (is.null(job)) return(cap)
  ## cores_total is parallelly::availableCores(): the job's CPUs, read from Slurm and its cgroup
  if (isTRUE(cap$cores_total < systemCores)) {
    cap$free_est <- cap$cores_total
    cap$loadavg[] <- 0
  }
  cap$mem_available_gb <- .minKnown(cap$mem_available_gb, job$memAvailableGB)
  cap$mem_total_gb <- .minKnown(cap$mem_total_gb, job$memLimitGB)
  cap
}

## The Slurm job a reservation id was made in (`@job<id>@` in it, see .newReservationId()); NA for none
.reservationJob <- function(id)
  ifelse(grepl("@job[0-9]+@", id), sub("^.*@job([0-9]+)@.*$", "\\1", id), NA_character_)
