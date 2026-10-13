## How fast is each host? DEoptimIterative() saves each fit's per-host evaluation speeds beside the
## core-reservation ledger (hostSpeed.rds), with the workers the fit had on each host, to calibrate
## .workerSpeed() against.

.hostSpeedFile <- function(path = getOption("clusters.reservationsPath"))
  file.path(dirname(reservationsPath(path)), "hostSpeed.rds")

.hostSpeedColumns <- c("host", "n", "median", "p90", "ratio", "time", "id", "workers", "booked")

## `speeds` rows newer than the window (`days` is getOption("clusters.hostSpeedDays", 30))
.hostSpeedRecent <- function(speeds, days = getOption("clusters.hostSpeedDays", 30), now = Sys.time())
  speeds[as.numeric(difftime(now, speeds$time, units = "days")) <= days, , drop = FALSE]

## The evaluation records that say which host ran them
.withHost <- function(evaluations)
  Filter(function(x) is.data.frame(x) && "host" %in% names(x), evaluations)

## Add `new` rows to the records in `file` (hostSpeed.rds, fitMemory.rds), dropping the rows of the same
## `id` and those older than the window. One lock for the read and the write.
.replaceRecords <- function(file, new, columns) {
  .withReservationLock(file, {
    old <- if (file.exists(file)) tryCatch(readRDS(file), error = function(e) NULL)
    ## rows from before a column existed (e.g. `workers`, added in 0.0.65) get NA there
    if (is.data.frame(old)) old[setdiff(columns, names(old))] <- NA
    old <- old[!old$id %in% new$id, columns, drop = FALSE]
    saveRDS(.hostSpeedRecent(rbind(old, new)), file)
  })
}

## The workers every live cluster has booked on each host, named as the evaluation records name the hosts
## (`nodename`), for `cl` built by plan_psock_min() (its "hostNodenames" attribute maps its ssh aliases);
## NULL for any other cluster, or when the ledger cannot be read. This is the load .workerSpeed() was
## calibrated against (booked workers, 2026-10-02), so .hostSpeedFactor() compares each measured ratio
## with the model at this count. A fit's own `workers` on a host says little about the host's load: in
## FireSense 13 fits share each host, 1-6 workers each, 14 on a 16-thread host.
.bookedByNodename <- function(cl, path = getOption("clusters.reservationsPath")) {
  tryCatch({
    map <- attr(cl, "hostNodenames", exact = TRUE)
    if (is.null(map) || !isTRUE(getOption("clusters.useReservations", TRUE))) return(NULL)
    res <- liveReservations(path)
    res$nodename <- unname(map[res$host])
    res <- res[!is.na(res$nodename), , drop = FALSE]
    if (!NROW(res)) return(NULL)
    c(tapply(as.integer(res$workers), res$nodename, sum))
  }, error = function(e) NULL)
}

## Append the per-host speeds of one chunk of evaluations to hostSpeed.rds. Called after every computed
## chunk, so a fit that is killed still leaves its records. `id` names the chunk (runName and chunk
## number): a chunk computed again, e.g. after a restart, replaces its earlier rows instead of adding a
## duplicate. `evaluations` is a list of `member$evaluations` data frames. `workers` is the workers this fit
## had on each host, `booked` the workers every cluster had booked there (see .bookedByNodename(); NA when
## not known). Never fails the fit.
.recordHostSpeed <- function(evaluations, id, booked = NULL, path = getOption("clusters.reservationsPath")) {
  tryCatch({
    evaluations <- .withHost(evaluations)
    if (!length(evaluations)) return(invisible(NULL))
    tab <- workerSpeed(evaluations, by = "host")
    perWorker <- workerSpeed(evaluations, by = "worker")
    tab$workers <- as.integer(table(perWorker$host)[tab$host])
    tab$booked <- if (length(booked)) as.integer(booked[tab$host]) else NA_integer_
    file <- .hostSpeedFile(path)
    new <- cbind(tab[setdiff(.hostSpeedColumns, c("time", "id"))], time = Sys.time(), id = id,
                 stringsAsFactors = FALSE)
    .replaceRecords(file, new, .hostSpeedColumns)
    invisible(new)
  }, error = function(e) {
    warning("Could not record host speeds: ", conditionMessage(e), call. = FALSE)
    invisible(NULL)
  })
}

## Show the per-host speeds of all the evaluations a fit computed
.showHostSpeed <- function(evaluations) {
  evaluations <- .withHost(evaluations)
  if (!length(evaluations)) return(invisible(NULL))
  message("Evaluation speed by host (ratio: host median over the median of all evaluations):")
  reproducible::messageDF(workerSpeed(evaluations, by = "host"))
}

## The id of one chunk's records: `<runName>_<iter>`. A fit without a run name (NULL or "") is named by its
## process, so concurrent unnamed fits do not overwrite each other's rows (FireSense, 2026-10-13: 13 fits
## all wrote id "_71").
.chunkId <- function(runName, iter) {
  name <- if (length(runName) && nzchar(as.character(runName)[1])) as.character(runName)[1]
          else paste0("pid", Sys.getpid())
  paste0(name, "_", iter)
}

## The recorded speeds, or NULL when there is no file or it cannot be read
.readHostSpeeds <- function(path = getOption("clusters.reservationsPath")) {
  file <- .hostSpeedFile(path)
  if (file.exists(file)) tryCatch(.withReservationLock(file, readRDS(file)), error = function(e) NULL)
}

#' How much faster or slower each host measured than the hardware model says
#'
#' [.workerSpeed()] predicts a host's speed from its cores and memory modules. The fits record how fast each
#' host really ran (`hostSpeed.rds`, see [workerSpeed()]): per chunk, each host's median evaluation time over
#' the median of all the chunk's evaluations (`ratio`), and the workers the fit had on the host (`workers`).
#' A host can be slower than the model says (pinus, 2026-10-13: 8 cores on 3 memory channels measured 1.45x
#' the generation median while the model predicted 0.87 of it), so the measured ratios correct the model.
#'
#' For every recorded chunk the model's own ratio is worked out the same way: each host's predicted
#' evaluation time at the workers every cluster had booked there when the chunk was recorded (`booked`, see
#' `.bookedByNodename()`), `1 / .workerSpeed(booked)`, over the weighted median of those times across the
#' chunk's evaluations. The correction is the measured ratio over the predicted one, so a host is penalised
#' only for being slower than the model expected at that load, and a host that ran faster than the model
#' expected at a heavy load is credited (mega, 2026-10-13: 63 workers booked on 80 threads, measured 1.07
#' where the model predicted about 1.3). A row without `booked` (recorded before this column) is compared
#' at the fit's own `workers` on the host, and one without either (before 0.0.65) with a predicted ratio of
#' 1.
#'
#' The log corrections of a host's rows are averaged, weighted by `n` evaluations and by age (a weight
#' halving every `halfLifeDays`: a host's speed changes with what else runs on it, and the last day says more
#' about the next hour than last week does; pinus measured 0.92-0.97 on 2026-10-01/02 and 1.44 on
#' 2026-10-13), and shrunk towards 0 by `N / (N + priorN)` with `N` the weighted evaluations, so one short
#' noisy record moves a host little (one chunk of 4 evaluations at ratio 3 gives 0.96, not 0.33). The factor
#' is `exp()` of that: 1 for a host as fast as the model says, below 1 for a slower one, above 1 for a faster
#' one, and 1 for a host with no record.
#'
#' @param nodes The node table (`host`, `nodename` as the records name it, `cores_total`, `cores_physical`,
#'   `dimms`).
#' @param speeds The data frame of `hostSpeed.rds` (`.readHostSpeeds()`), or `NULL`.
#' @param beta What a hyperthread adds to a core (see [.workerSpeed()]).
#' @param days Only rows newer than this many days count; `options(clusters.hostSpeedDays)`, default 30.
#' @param halfLifeDays A row's weight halves every this many days; `options(clusters.hostSpeedHalfLifeDays)`,
#'   default 1.
#' @param priorN Evaluations at which a host's correction is half applied; `options(clusters.hostSpeedPriorN)`,
#'   default 100.
#' @param now The time rows are aged from.
#' @return A numeric vector, one factor per row of `nodes`.
#' @keywords internal
.hostSpeedFactor <- function(nodes, speeds, beta = 0.75,
                             days = getOption("clusters.hostSpeedDays", 30),
                             halfLifeDays = getOption("clusters.hostSpeedHalfLifeDays", 1),
                             priorN = getOption("clusters.hostSpeedPriorN", 100), now = Sys.time()) {
  ones <- rep(1, NROW(nodes))
  if (!is.data.frame(speeds) || !NROW(speeds) || !all(c("host", "n", "ratio", "time", "id") %in% names(speeds)))
    return(ones)
  s <- .hostSpeedRecent(speeds, days, now)
  s <- s[is.finite(s$ratio) & s$ratio > 0 & is.finite(s$n) & s$n > 0, , drop = FALSE]
  if (!NROW(s)) return(ones)
  nodeNames <- if ("nodename" %in% names(nodes)) nodes$nodename else nodes$host
  i <- match(s$host, nodeNames)
  ## predicted evaluation time of each row at the workers booked on the host, else this fit's workers;
  ## 1 (no prediction) without either or for a host not in `nodes`
  workers <- if ("workers" %in% names(s)) as.numeric(s$workers) else rep(NA_real_, NROW(s))
  if ("booked" %in% names(s)) workers <- ifelse(is.na(s$booked), workers, as.numeric(s$booked))
  col <- function(name) if (name %in% names(nodes)) as.numeric(nodes[[name]])[i] else rep(NA_real_, NROW(s))
  predTime <- rep(1, NROW(s))
  known <- !is.na(i) & !is.na(workers) & !is.na(col("cores_total"))
  predTime[known] <- 1 / .workerSpeed(workers[known], col("dimms")[known], col("cores_physical")[known],
                                      col("cores_total")[known], beta)
  wmedian <- function(x, w) { o <- order(x); x <- x[o]; w <- w[o]; x[which(cumsum(w) >= sum(w) / 2)[1]] }
  predRatio <- stats::ave(seq_len(NROW(s)), s$id, FUN = function(j) predTime[j] / wmedian(predTime[j], s$n[j]))
  logError <- log(s$ratio) - log(predRatio)
  weight <- s$n * 2^(-pmax(as.numeric(difftime(now, s$time, units = "days")), 0) / halfLifeDays)
  vapply(seq_len(NROW(nodes)), function(k) {
    rows <- which(i == k)
    if (!length(rows)) return(1)
    N <- sum(weight[rows])
    if (!is.finite(N) || N <= 0) return(1)
    exp(-stats::weighted.mean(logError[rows], weight[rows]) * N / (N + priorN))
  }, numeric(1))
}
