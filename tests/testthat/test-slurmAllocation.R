## A Slurm job given part of a shared node must size its cluster from its allocation (2026-10-28: a job of
## 41 CPUs and 200 GB on a 64-CPU, 490 GB node probed the node: 64 threads, the node's load average, the
## node's free memory).

## The cgroup files of a job: <root>/<jobDir>/memory.max and memory.current, and the process's own line
## in /proc/self/cgroup, which names the step below it
mockCgroup <- function(max = "214748364800", current = "21474836480", stepMax = "max", inactive = 0,
                       env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = env)
  step <- file.path(root, "system.slice", "slurmstepd.scope", "job_77", "step_0")
  dir.create(step, recursive = TRUE)
  job <- dirname(step)
  writeLines(max, file.path(job, "memory.max"))
  writeLines(current, file.path(job, "memory.current"))
  writeLines(c("anon 1", paste("inactive_file", inactive)), file.path(job, "memory.stat"))
  writeLines(stepMax, file.path(step, "memory.max"))
  writeLines("1", file.path(step, "memory.current"))
  proc <- file.path(root, "self.cgroup")
  writeLines("0::/system.slice/slurmstepd.scope/job_77/step_0", proc)
  list(root = root, proc = proc)
}

## What the node says of itself: 64 threads, 50 of load from other users' jobs, 300 of 490 GB available
nodeCap <- function(cores = 64L, avail = 300, total = 490, load = 50)
  list(nodename = Sys.info()[["nodename"]], cores_total = cores, cores_physical = cores / 2, dimms = 16L,
       free_est = as.integer(floor(0.9 * (cores - load))), loadavg = c("1min" = load, "5min" = load, "15min" = load),
       mem_available_gb = avail, mem_total_gb = total)

test_that("outside Slurm there is no job allocation and the probe is unchanged", {
  withr::local_envvar(SLURM_JOB_ID = NA, SLURM_CPUS_ON_NODE = NA, SLURM_MEM_PER_NODE = NA)
  expect_null(clusters:::.slurmAllocation())
  cap <- nodeCap()
  expect_identical(clusters:::.limitToJob(cap, NULL, systemCores = 64L), cap)
})

test_that("the allocation is read from Slurm's variables and the job's cgroup", {
  cg <- mockCgroup()
  withr::local_envvar(SLURM_JOB_ID = "77", SLURM_CPUS_ON_NODE = "41", SLURM_MEM_PER_NODE = NA)
  job <- clusters:::.slurmAllocation(procCgroup = cg$proc, cgroupRoot = cg$root)
  expect_equal(job$cores, 41)
  ## memory.max (200 GiB) is on the job's cgroup, the step below it says "max"; 20 GiB used
  expect_equal(job$memLimitGB, 200)
  expect_equal(job$memAvailableGB, 180)
  ## page cache the kernel can drop is not use (memory.current counts it)
  cg2 <- mockCgroup(inactive = 10 * 1024^3)
  job2 <- clusters:::.slurmAllocation(procCgroup = cg2$proc, cgroupRoot = cg2$root)
  expect_equal(job2$memAvailableGB, 190)
})

test_that("without a cgroup limit the job's memory is SLURM_MEM_PER_NODE (MB)", {
  none <- file.path(withr::local_tempdir(), "absent")
  withr::local_envvar(SLURM_JOB_ID = "77", SLURM_CPUS_ON_NODE = "41", SLURM_MEM_PER_NODE = "204800")
  job <- clusters:::.slurmAllocation(procCgroup = none, cgroupRoot = none)
  expect_equal(job$memLimitGB, 200)
  expect_equal(job$memAvailableGB, 200)
  ## per CPU, when the job asked that way
  withr::local_envvar(SLURM_MEM_PER_NODE = NA, SLURM_MEM_PER_CPU = "5120")
  expect_equal(clusters:::.slurmAllocation(procCgroup = none, cgroupRoot = none)$memLimitGB, 41 * 5)
  ## nothing known: no memory cap
  withr::local_envvar(SLURM_MEM_PER_CPU = NA)
  job <- clusters:::.slurmAllocation(procCgroup = none, cgroupRoot = none)
  expect_true(is.na(job$memLimitGB) && is.na(job$memAvailableGB))
})

test_that("41 CPUs and 200 GB of a 64-CPU node give at most 41 - keepFreeCores workers and 200 GB", {
  withLedger()
  cg <- mockCgroup()
  withr::local_envvar(SLURM_JOB_ID = "77", SLURM_CPUS_ON_NODE = "41", SLURM_MEM_PER_NODE = NA)
  job <- clusters:::.slurmAllocation(procCgroup = cg$proc, cgroupRoot = cg$root)
  limited <- clusters:::.limitToJob(nodeCap(), job, systemCores = 64L)
  expect_equal(limited$cores_total, 41L)
  expect_lte(limited$cores_physical, 41)
  expect_lte(limited$mem_total_gb, 200)
  expect_lte(limited$mem_available_gb, 200)
  ## other users' load on the node is not this job's
  expect_true(all(limited$loadavg == 0))
  nodes <- clusters:::.nodeTable("localhost", list(limited))
  keep <- getOption("clusters.keepFreeCores", 2)
  got <- suppressMessages(clusters:::.fitCapacity(nodes, total = 100, load_memory = "5min"))
  expect_equal(got$free_est, 41 - keep)
  ## 12 GB workers: (180 GB available - 10% of 200 GB) / 12 = 13, fewer than the cores allow
  got <- suppressMessages(clusters:::.fitCapacity(nodes, total = 100, load_memory = "5min", memPerWorkerGB = 12))
  expect_equal(got$free_est, floor((180 - 0.1 * 200) / 12))
  ## the node as the probe saw it, unlimited, gave the job nothing (load 50 of 64) and counted the node's memory
  whole <- clusters:::.nodeTable("localhost", list(nodeCap()))
  expect_lt(suppressMessages(clusters:::.fitCapacity(whole, total = 100, load_memory = "5min"))$free_est, 41 - keep)
})

test_that("the job's own workers, in the ledger, still come off its allocation", {
  withLedger()
  withr::local_envvar(SLURM_JOB_ID = "77")
  job <- list(cores = 41, memLimitGB = 200, memAvailableGB = 200)
  nodes <- clusters:::.nodeTable("localhost", list(clusters:::.limitToJob(nodeCap(), job, systemCores = 64L)))
  reserveCores(data.frame(host = "localhost", assign = 20L), id = clusters:::.newReservationId())
  got <- suppressMessages(clusters:::.fitCapacity(nodes, total = 100, load_memory = "5min"))
  expect_equal(got$reserved, 20L)
  expect_equal(got$free_est, 41 - 2 - 20)
})

test_that("another job's booking on the same node is not this job's", {
  withLedger()
  job <- list(cores = 41, memLimitGB = 200, memAvailableGB = 200)
  nodes <- clusters:::.nodeTable("localhost", list(clusters:::.limitToJob(nodeCap(), job, systemCores = 64L)))
  withr::with_envvar(c(SLURM_JOB_ID = "88"),
                     reserveCores(data.frame(host = "localhost", assign = 20L), id = clusters:::.newReservationId()))
  ## a booking with no job (another version's, or outside Slurm) cannot be told apart and counts
  reserveCores(data.frame(host = "localhost", assign = 3L), id = paste0(Sys.info()[["nodename"]], ":resv1"))
  withr::with_envvar(c(SLURM_JOB_ID = "77"), {
    got <- suppressMessages(clusters:::.fitCapacity(nodes, total = 100, load_memory = "5min"))
    expect_equal(got$reserved, 3L)
    expect_equal(got$free_est, 41 - 2 - 3)
  })
  ## outside Slurm all three count, as before
  withr::with_envvar(c(SLURM_JOB_ID = NA), {
    got <- suppressMessages(clusters:::.fitCapacity(nodes, total = 100, load_memory = "5min"))
    expect_equal(got$reserved, 23L)
  })
})

test_that("a job given the whole node, and a probe outside Slurm, are as before", {
  withLedger()
  cg <- mockCgroup(max = as.character(490 * 1024^3), current = as.character(5 * 1024^3))
  withr::local_envvar(SLURM_JOB_ID = "77", SLURM_CPUS_ON_NODE = "64", SLURM_MEM_PER_NODE = NA)
  job <- clusters:::.slurmAllocation(procCgroup = cg$proc, cgroupRoot = cg$root)
  cap <- nodeCap(avail = 480, load = 10)
  expect_identical(clusters:::.limitToJob(cap, job, systemCores = 64L), cap)
  ## the same node with less memory free than the cgroup says (other processes) is not raised
  cap <- nodeCap(avail = 100, load = 10)
  expect_identical(clusters:::.limitToJob(cap, job, systemCores = 64L), cap)
  ## the live probe outside Slurm
  withr::local_envvar(SLURM_JOB_ID = NA)
  cap <- clusters:::.probeHost("5min", 0.9)
  expect_equal(cap$cores_total, as.integer(parallelly::availableCores()))
})
