## A cluster is sized once, when it is built, from the hosts' load at that moment. A fit then keeps it for
## hours, so a host that was busy at the build stays unused after it frees up, and the hosts that were free
## stay crowded. FireSense held-out fits, 2026-10-02: the master host (80 threads) carried 2 of 520 workers
## because each fit was built while the masters' threshold calibration loaded it (load 50-500), which ended
## minutes later; hosts with 48 threads carried 50-52. Between generations, a cluster asks again where its
## workers belong, by the rule of a new build (.fitCapacity()), and moves them.

#' Which workers move where
#'
#' @param hosts The host of each worker, in cluster order.
#' @param target Named integer: the workers each host should have (`.speedAllocate()`'s `assign`).
#' @return A list: `remove`, the positions of the workers to stop, from the hosts most over their target
#'   first, and `add`, the hosts to start their replacements on, as many as `remove`. A host short of its
#'   target receives only up to what is surplus elsewhere, and a surplus moves only to a host that is short.
#' @keywords internal
.rebalanceMoves <- function(hosts, target) {
  all <- union(names(target), hosts)
  have <- vapply(all, function(h) sum(hosts == h), numeric(1))
  want <- vapply(all, function(h) if (h %in% names(target)) as.numeric(target[[h]]) else 0, numeric(1))
  surplus <- pmax(have - want, 0)
  short <- pmax(want - have, 0)
  n <- min(sum(surplus), sum(short))
  if (n == 0) return(list(remove = integer(0), add = character(0)))
  remove <- integer(0)
  for (h in all[order(-surplus)]) {
    k <- min(surplus[[h]], n - length(remove))
    if (k <= 0) break
    remove <- c(remove, utils::tail(which(hosts == h), k))
  }
  add <- character(0)
  for (h in all[order(-short)]) {
    k <- min(short[[h]], n - length(add))
    if (k <= 0) break
    add <- c(add, rep(h, k))
  }
  list(remove = remove, add = add)
}

#' A function that moves a running cluster's workers to where a new build would put them
#'
#' Probes the hosts as the build did, works out with `.fitCapacity()` and [.speedAllocate()] where this
#' cluster's workers belong, counting every other cluster's reservation but not its own, and moves the
#' workers that are elsewhere: new workers are started and given what [clusterSetup()] gave the first ones
#' (as [.restartClusterFn()] does), then swapped in for the old, which are stopped. The reservation is
#' re-booked before the new workers start, under the lock every build allocates under, so no other build
#' or rebalance decides between this cluster's probe and its re-booking. Nothing moves when fewer than
#' `options(clusters.rebalanceMinMoves)` workers (default a tenth of the cluster) would. A failure leaves
#' the cluster as it was, with a warning: a fit must not stop because its workers could not move.
#' Stored on the cluster as the attribute `"rebalance"`; [DEoptimIterative()] calls it every
#' `options(clusters.rebalanceEvery)` generations (default 100).
#'
#' @param plan What [plan_psock_min()] returned: `startProbe`, `capacity`, `startNodes`, `beta`.
#' @param digest What [shippedObjectsDigest()] gave at set-up: the objects must not have changed since.
#' @inheritParams .restartClusterFn
#' @keywords internal
.rebalanceFn <- function(plan, pkgsNeeded, objsNeeded, envir, digest) {
  force(plan); force(pkgsNeeded); force(objsNeeded); force(envir); force(digest)
  self <- function(cl) {
    orig <- cl   # handed back unchanged if anything fails
    hosts <- vapply(cl, function(node) as.character(node$host)[1], character(1))
    minMoves <- getOption("clusters.rebalanceMinMoves", ceiling(length(cl) / 10))
    token <- attr(cl, "reservationToken", exact = TRUE)
    ownId <- if (is.environment(token)) token$id
    out <- tryCatch({
      if (!is.null(digest) &&
          !identical(reproducible::.robustDigest(mget(sort(unlist(objsNeeded)), envir = envir)), digest))
        stop("the objects sent to the workers have changed since clusterSetup()")
      probe <- plan$startProbe()
      on.exit(.stopCluster(probe), add = TRUE)
      ## decided and re-booked under the lock every build allocates under (.withAllocationLock())
      moves <- .withAllocationLock({
        nodes <- plan$capacity(probe, own = table(hosts), ownId = ownId)
        target <- .speedAllocate(nodes, total = length(cl), beta = plan$beta)
        m <- .rebalanceMoves(hosts, stats::setNames(target$assign, target$host))
        if (length(m$remove) >= max(1, minMoves) && !is.null(ownId)) {
          newHosts <- hosts
          newHosts[m$remove] <- m$add
          booked <- as.data.frame(table(host = newHosts), stringsAsFactors = FALSE)
          names(booked)[2] <- "assign"
          .rebookCores(ownId, booked)
        }
        m
      })
      if (length(moves$remove) < max(1, minMoves)) {
        message("clusters: ", length(moves$remove), " worker(s) would move (fewer than ", max(1, minMoves),
                "); workers stay where they are (", .nodeHosts(cl), ")")
        return(cl)
      }
      newHosts <- hosts
      newHosts[moves$remove] <- moves$add
      fresh <- NULL
      done <- FALSE
      on.exit(if (!done) {
        if (!is.null(fresh)) .stopNodes(fresh)
        if (!is.null(ownId)) {
          was <- as.data.frame(table(host = hosts), stringsAsFactors = FALSE)
          names(was)[2] <- "assign"
          .rebookCores(ownId, was)
        }
      }, add = TRUE)
      fresh <- plan$startNodes(moves$add, autoStop = FALSE)
      fresh <- .replaceDeadNodes(fresh, function(host) plan$startNodes(host, autoStop = FALSE),
                                 action = "stop")$cluster
      .shipToWorkers(fresh, moves$add, pkgsNeeded, objsNeeded, envir)
      .setWorkerTimeout(fresh)
      old <- cl[moves$remove]
      for (k in seq_along(moves$remove)) cl[[moves$remove[k]]] <- fresh[[k]]
      done <- TRUE
      .stopNodes(old)
      ## autoStop and plan_psock_min()'s exit handler must stop these nodes, not the closed ones
      gcMe <- attr(cl, "gcMe")
      if (is.environment(gcMe)) gcMe$cluster <- cl
      current <- attr(cl, "currentCluster", exact = TRUE)
      if (is.environment(current)) current$cluster <- cl
      ## a rebuild after a dead worker starts the workers where they are now
      attr(cl, "restartCluster") <- .restartClusterFn(plan$startNodes, newHosts, pkgsNeeded, objsNeeded,
                                                      envir, digest, token)
      message("clusters: moved ", length(moves$remove), " worker(s): ",
              paste(names(table(hosts[moves$remove])), "x", table(hosts[moves$remove]), collapse = ", "),
              " -> ", paste(names(table(moves$add)), "x", table(moves$add), collapse = ", "),
              "; now ", .nodeHosts(cl))
      cl
    }, error = function(e) {
      warning("clusters: could not move workers (", conditionMessage(e), "); they stay where they are",
              call. = FALSE)
      orig
    })
    out
  }
  self
}
