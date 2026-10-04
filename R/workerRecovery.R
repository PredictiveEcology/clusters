## A PSOCK worker that dies without closing its socket (its ssh tunnel stays up, or its process is
## stopped) leaves the master blocked in unserialize() for as long as the socket's timeout, which
## parallelly sets to 30 days. FireSense, 2026-09-29: one of 40 workers died at start-up and the master
## sat at 0% CPU for 35 minutes until it was interrupted by hand. These functions bound that wait, find
## the node that cannot answer, and replace it (at set-up) or rebuild the cluster (mid-run).

## Seconds a master waits for a worker's reply before it calls the worker dead. It must comfortably
## exceed the slowest objective-function evaluation: a FireSense evaluation takes seconds to a minute.
.workerTimeout <- function() {
  x <- getOption("clusters.workerTimeout", 3600)
  if (!is.numeric(x) || length(x) != 1L || is.na(x) || x <= 0)
    stop("options(clusters.workerTimeout) must be one positive number of seconds")
  x
}

## Replace the 30-day read timeout of every socket node of `cl`.
.setWorkerTimeout <- function(cl, seconds = .workerTimeout()) {
  for (node in cl) if (inherits(node$con, "sockconn")) socketTimeout(node$con, seconds)
  invisible(cl)
}

## The same, at the workers' end of the sockets. parallelly gives each worker its `timeout` as the
## worker's read timeout (TIMEOUT=), and makeClusterPSOCK() passes `connectTimeout` there to bound the
## connect, and sets this as soon as the worker has connected. Left at `connectTimeout`, a worker
## quit after that many seconds without a call from the master, and the master's next call failed
## with "error reading from connection" (FireSense, 2026-10-03: fits died after start-up workers were
## replaced, which left the others idle for over 2 minutes).
.setWorkerSideTimeout <- function(cl, seconds) {
  socks <- which(vapply(cl, function(node) inherits(node$con, "sockconn"), logical(1)))
  if (!length(socks)) return(invisible(cl))
  parallel::clusterCall(cl[socks], function(seconds) {
    for (i in getAllConnections()) {
      con <- getConnection(i)
      if (inherits(con, "sockconn")) socketTimeout(con, seconds)
    }
    NULL
  }, seconds)
  invisible(cl)
}

## Does node `i` of `cl` return a trivial call within `seconds`?
.nodeAnswers <- function(cl, i, seconds) {
  con <- cl[[i]]$con
  if (inherits(con, "sockconn")) {
    old <- try(socketTimeout(con, seconds), silent = TRUE)
    if (inherits(old, "try-error")) return(FALSE)
    on.exit(try(socketTimeout(con, old), silent = TRUE), add = TRUE)
  }
  isTRUE(tryCatch(parallel::clusterCall(cl[i], function() TRUE)[[1]], error = function(e) FALSE))
}

.deadNodes <- function(cl, seconds) {
  which(!vapply(seq_along(cl), function(i) .nodeAnswers(cl, i, seconds), logical(1)))
}

.nodeHosts <- function(cl) {
  hosts <- vapply(cl, function(node) as.character(node$host)[1], character(1))
  paste(paste0(names(table(hosts)), " x", as.integer(table(hosts))), collapse = ", ")
}

## A cluster made with parallelly's autoStop = TRUE is stopped again when it is garbage collected. If it
## was already stopped, that second stop sends "DONE" to, and closes, whatever connections now have its
## old connection numbers: R reuses them, and close() does not check that a connection object is current
## (CI, 2026-10-01: a test's live worker got "invalid connection"). Every stop here disarms it first.
.disarmAutoStop <- function(cl) {
  gcMe <- attr(cl, "gcMe")
  if (is.environment(gcMe)) gcMe$cluster <- structure(list(), class = c("SOCKcluster", "cluster"))
  invisible(cl)
}

## Is this node's connection still open? Once closed, its number can be given to a new connection, and
## close() or a "DONE" sent through the old connection object reaches that one instead.
.nodeOpen <- function(node) {
  isTRUE(tryCatch(identical(attr(getConnection(as.integer(node$con)), "conn_id"), attr(node$con, "conn_id")),
                  error = function(e) FALSE))
}

.stopCluster <- function(cl) {
  .disarmAutoStop(cl)
  try(parallel::stopCluster(cl[vapply(cl, .nodeOpen, logical(1))]), silent = TRUE)
  invisible(NULL)
}

.stopNodes <- function(cl) {
  .disarmAutoStop(cl)
  for (node in Filter(.nodeOpen, cl)) try(close(node$con), silent = TRUE)
  invisible(NULL)
}

#' Replace, or drop, the nodes of a fresh cluster that do not answer
#'
#' Sends every node a trivial call. A node that does not answer within `seconds` is closed and
#' replaced by one started with `start(host)`, up to `tries` times. Run at set-up, before anything is
#' sent to the workers, so a node's stream holds nothing but its own reply.
#'
#' @param cl A cluster.
#' @param start A function of one host name that returns a one-node cluster.
#' @param seconds Seconds a node has to answer.
#' @param tries How many replacements are tried for a node.
#' @param action `"stop"` (default) if a node cannot be replaced, or `"drop"` to go on without it;
#'   `options(clusters.onDeadWorker)`.
#' @return A list: `cluster`, and `dropped`, the positions of the nodes that were dropped.
#' @keywords internal
.replaceDeadNodes <- function(cl, start, seconds = getOption("clusters.pingTimeout", 30),
                              tries = getOption("clusters.workerRetries", 2L),
                              action = getOption("clusters.onDeadWorker", "stop")) {
  action <- match.arg(action, c("stop", "drop"))
  dead <- .deadNodes(cl, seconds)
  if (!length(dead)) return(list(cluster = cl, dropped = integer(0)))
  hosts <- vapply(dead, function(i) as.character(cl[[i]]$host)[1], character(1))
  message("clusters: ", length(dead), " worker(s) did not answer within ", seconds, " s (",
          paste(unique(hosts), collapse = ", "), "); replacing")
  dropped <- integer(0)
  for (k in seq_along(dead)) {
    i <- dead[k]
    try(close(cl[[i]]$con), silent = TRUE)
    fresh <- NULL
    for (attempt in seq_len(tries)) {
      cand <- tryCatch(start(hosts[k]), error = function(e) {
        message("clusters: starting a replacement worker on ", hosts[k], " failed: ", conditionMessage(e))
        NULL
      })
      if (is.null(cand)) next
      if (!length(.deadNodes(cand, seconds))) { fresh <- cand; break }
      .stopNodes(cand)
    }
    if (is.null(fresh)) {
      if (identical(action, "stop"))
        stop("clusters: a worker on ", hosts[k], " does not answer and ", tries,
             " replacement(s) did not either. options(clusters.onDeadWorker = \"drop\") goes on ",
             "with fewer workers.", call. = FALSE)
      message("clusters: dropping the worker on ", hosts[k])
      dropped <- c(dropped, i)
    } else {
      message("clusters: replaced the worker on ", hosts[k])
      cl[[i]] <- fresh[[1]]
    }
  }
  if (length(dropped)) {
    kept <- attributes(cl)
    cl <- cl[-dropped]
    for (a in setdiff(names(kept), c("names", "class"))) attr(cl, a) <- kept[[a]]
  }
  ## autoStop (see .disarmAutoStop()) must stop these nodes, not the closed ones they replaced
  gcMe <- attr(cl, "gcMe")
  if (is.environment(gcMe)) gcMe$cluster <- cl
  list(cluster = cl, dropped = dropped)
}

## An error from a lost connection, as opposed to one raised by the objective function on a worker
## (parallel reports those as "one node produced an error: ...").
.isConnectionError <- function(e) {
  msg <- conditionMessage(e)
  grepl("connection", msg, fixed = TRUE) && !grepl("produced an error|produced errors", msg)
}

#' A function that replaces a cluster with a new, prepared one
#'
#' Stops the old cluster, starts `cores` again, replaces any node that does not answer, and sends the
#' workers what [clusterSetup()] sent the first ones. Stored on the cluster as the attribute
#' `"restartCluster"` for [.runWithRebuild()].
#'
#' @param startNodes A function of a vector of hosts (and `autoStop`) that starts a cluster.
#' @param digest What [shippedObjectsDigest()] gave at set-up: the objects must not have changed since.
#' @param token The old cluster's `"reservationToken"`, kept alive on the new one.
#' @keywords internal
.restartClusterFn <- function(startNodes, cores, pkgsNeeded, objsNeeded, envir, digest, token) {
  force(startNodes); force(cores); force(pkgsNeeded); force(objsNeeded); force(envir)
  force(digest); force(token)
  self <- function(old) {
    if (!is.null(digest) &&
        !identical(reproducible::.robustDigest(mget(sort(unlist(objsNeeded)), envir = envir)), digest))
      stop("the objects sent to the workers have changed since clusterSetup(); cannot rebuild the cluster")
    .stopNodes(old)
    fresh <- startNodes(cores)
    ok <- FALSE
    on.exit(if (!ok) .stopNodes(fresh), add = TRUE)
    fresh <- .replaceDeadNodes(fresh, function(host) startNodes(host, autoStop = FALSE),
                               action = "stop")$cluster
    .shipToWorkers(fresh, cores, pkgsNeeded, objsNeeded, envir)
    .setWorkerTimeout(fresh)
    attr(fresh, "reservationToken") <- token
    attr(fresh, "restartCluster") <- self
    attr(fresh, "rebalance") <- attr(old, "rebalance", exact = TRUE)
    ## plan_psock_min()'s exit handler stops whichever cluster this is (see "currentCluster" there)
    current <- attr(old, "currentCluster", exact = TRUE)
    if (is.environment(current)) {
      attr(fresh, "currentCluster") <- current
      current$cluster <- fresh
    }
    ok <- TRUE
    fresh
  }
  self
}

#' Run `run(cl)`; if a worker connection fails, rebuild the cluster and run it again
#'
#' A worker that dies mid-run leaves the replies of the others unread in their sockets, so the
#' surviving nodes cannot be reused: the whole cluster is replaced, using the `"restartCluster"`
#' attribute that [clusterSetup()] puts on it, and `run()` is called again on the new one. A failure
#' inside the objective function is not a connection failure and is raised as it was.
#'
#' @param cl A cluster, or `NULL`.
#' @param run A function of a cluster.
#' @param retries How many rebuilds are tried; `options(clusters.workerRetries)`.
#' @return A list: `value`, what `run()` returned, and `cluster`, the cluster it last ran on.
#' @keywords internal
.runWithRebuild <- function(cl, run, retries = getOption("clusters.workerRetries", 2L)) {
  attempt <- 0L
  repeat {
    res <- tryCatch(list(value = run(cl)), error = function(e) e)
    if (!inherits(res, "error")) return(list(value = res$value, cluster = cl))
    if (is.null(cl) || !.isConnectionError(res)) stop(res)
    restart <- attr(cl, "restartCluster", exact = TRUE)
    if (is.null(restart) || attempt >= retries)
      stop("clusters: a worker connection failed (workers: ", .nodeHosts(cl), "; ",
           if (is.null(restart)) "the cluster cannot be rebuilt" else paste(attempt, "rebuild(s) tried"),
           "; options(clusters.workerTimeout) is ", .workerTimeout(), " s): ", conditionMessage(res),
           call. = FALSE)
    attempt <- attempt + 1L
    message("clusters: a worker connection failed (", conditionMessage(res), "); rebuilding the cluster (",
            attempt, " of ", retries, ") and running this generation again")
    cl <- tryCatch(restart(cl), error = function(e)
      stop("clusters: could not rebuild the cluster after a worker connection failed (workers: ",
           .nodeHosts(cl), "): ", conditionMessage(e), call. = FALSE))
  }
}
