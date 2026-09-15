## First port of this machine's ephemeral range: the ports the kernel hands to outgoing connections.
## Linux's default range is 32768-60999; every FireSense host uses it.
.ephemeralPortStart <- function(file = "/proc/sys/net/ipv4/ip_local_port_range") {
  first <- if (file.exists(file)) {
    suppressWarnings(as.integer(strsplit(trimws(readLines(file, n = 1L, warn = FALSE)), "\\s+")[[1]][1]))
  } else {
    NA_integer_
  }
  if (length(first) != 1L || is.na(first)) 32768L else first
}

## A random block of candidate ports for the master. parallelly listens on one of them and gives each
## worker's reverse tunnel the next ports after it, on the worker's host. The whole block, plus one port
## per worker, must end below the ephemeral range: a tunnel port inside it can already be held by any
## connection on that host, and then the tunnel fails and its worker never reaches the master. On
## 2026-09-15 a 110-worker build hung that way, its block drawn from 20000:40000.
.tunnelPortBlock <- function(workers, blockSize = 300L, lowest = 20000L) {
  nWorkers <- if (is.numeric(workers) && length(workers) == 1L) as.integer(workers) else length(workers)
  lastStart <- .ephemeralPortStart() - blockSize - nWorkers
  start <- lowest + sample.int(max(1L, lastStart - lowest + 1L), 1L) - 1L
  seq.int(start, length.out = blockSize)
}
