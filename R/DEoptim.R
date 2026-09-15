

## A per-process record of objective-function evaluations. DEoptim() returns the final population
## but not its objective values, so each new evaluation -- its value and how long it took -- is
## recorded where it runs (this process, or a PSOCK worker) and gathered after every chunk.
.deoptimRecord <- new.env(parent = emptyenv())

.parKey <- function(par) paste(sprintf("%a", as.numeric(par)), collapse = ",")

.deoptimRecordTake <- function() {
  out <- list(keys = .deoptimRecord$keys, vals = .deoptimRecord$vals, secs = .deoptimRecord$secs)
  .deoptimRecord$keys <- NULL
  .deoptimRecord$vals <- NULL
  .deoptimRecord$secs <- NULL
  out
}

## Take, and clear, the records of this process and of every worker of `cl`.
.deoptimRecordTakeAll <- function(cl = NULL) {
  recs <- list(.deoptimRecordTake())
  if (!is.null(cl))
    recs <- c(recs, parallel::clusterCall(cl, .deoptimRecordTake))
  list(keys = unlist(lapply(recs, `[[`, "keys")), vals = unlist(lapply(recs, `[[`, "vals")),
       secs = unlist(lapply(recs, `[[`, "secs")))
}

## `fn`, except that a parameter set whose value is already known returns that value instead of
## being evaluated again, and every new evaluation is recorded with its elapsed seconds. Built in its
## own small frame, not in the caller's, because DEoptim sends this closure to the workers.
.memoObjFun <- function(fn, knownKeys, knownVals) {
  force(fn); force(knownKeys); force(knownVals)
  function(par, ...) {
    key <- .parKey(par)
    hit <- match(key, knownKeys)
    if (!is.na(hit)) return(knownVals[[hit]])
    started <- proc.time()[["elapsed"]]
    val <- fn(par, ...)
    .deoptimRecord$secs <- c(.deoptimRecord$secs, proc.time()[["elapsed"]] - started)
    .deoptimRecord$keys <- c(.deoptimRecord$keys, key)
    .deoptimRecord$vals <- c(.deoptimRecord$vals, val)
    val
  }
}

## `control$itermax` DEoptim generations from `control$initialpop` that cost only the new trial
## evaluations: DEoptim evaluates its initial population first, and the carried population's values
## (`known`) come from the lookup instead. The final population's values are returned as
## `member$popval`, which seeds the next chunk, also when this chunk is loaded from the cache; every
## new evaluation's seconds and value as `member$evaluations`.
.DEoptimChunk <- function(fn, lower, upper, control, known, dotsList) {
  cl <- control$cluster
  invisible(.deoptimRecordTakeAll(cl))   # nothing left over from an interrupted chunk
  memoFn <- .memoObjFun(fn, known$keys, known$vals)
  out <- do.call(DEoptim::DEoptim,
                 c(list(fn = memoFn, lower = lower, upper = upper, control = control), dotsList))
  recs <- .deoptimRecordTakeAll(cl)
  keys <- c(known$keys, recs$keys)
  vals <- c(known$vals, recs$vals)
  out$member$popval <- unname(vals[match(apply(out$member$pop, 1, .parKey), keys)])
  out$member$evaluations <- data.frame(seconds = as.numeric(recs$secs), value = as.numeric(recs$vals))
  out
}

## One line for a chunk's evaluation times, e.g. "880 evaluations: 1.2 / 3.4 / 9.8 / 25.1 s (min / median /
## 90% / max); 11.2 s per generation".
.evaluationTimes <- function(evaluations, generations, wallSeconds) {
  s <- evaluations$seconds
  q <- if (length(s)) round(stats::quantile(s, c(0, 0.5, 0.9, 1), names = FALSE), 1) else rep(NA, 4)
  paste0(length(s), " evaluations: ", paste(q, collapse = " / "), " s (min / median / 90% / max); ",
         round(wallSeconds / max(1, generations), 1), " s per generation")
}

## NP for a cluster of `nWorkers`. DEoptim evaluates the population one member per worker, so a
## larger NP leaves members queued behind busy workers and a smaller one leaves workers idle.
.clusterNP <- function(NP, nWorkers) {
  nWorkers <- as.integer(nWorkers)
  if (!length(nWorkers) || nWorkers == 0L) return(NP)
  if (nWorkers < 4L)
    stop("DEoptim needs at least 4 population members, one per worker, but the cluster has only ",
         nWorkers, " worker(s). Wait for free cores (options(clusters.waitForCores = )) or run ",
         "fewer fits at once.", call. = FALSE)
  if (!is.null(NP) && !is.na(NP) && !identical(as.integer(NP), nWorkers))
    message("NP set to ", nWorkers, ", the number of workers in the cluster (", NP, " was requested)")
  nWorkers
}

## DEoptim settings a caller passes through clusterSetup(). Every name must be a DEoptim.control()
## argument: a misspelt setting silently falling back to a default is how `.c` went unused.
## `cluster` and `parallelType` are this package's to set.
.deoptimControlArgs <- function(controlArgs) {
  if (is.null(controlArgs) || !length(controlArgs)) return(list())
  controlArgs <- as.list(controlArgs)
  nms <- names(controlArgs)
  if (is.null(nms) || any(!nzchar(nms)))
    stop("controlArgs must be a named list of DEoptim.control() settings", call. = FALSE)
  known <- setdiff(names(formals(DEoptim::DEoptim.control)), c("cluster", "parallelType"))
  bad <- setdiff(nms, known)
  if (length(bad))
    stop("Not DEoptim.control() settings: ", paste(bad, collapse = ", "),
         ". Valid names: ", paste(known, collapse = ", "), call. = FALSE)
  controlArgs
}

DEoptimIterative2 <- function(fn, lower, upper, control, ...,
                              # formulaToFit, covMinMax, tests, maxFireSpread, mutuallyExclusive,
                              # doObjFunAssertions, Nreps, objFunCoresInternal, thresh, rep,
                              .plots, figurePath, cachePath, runName = 1, .verbose = TRUE) {
  DE <- list()
  ## progress plots are drawn with SpaDES.core::Plots(): say so now, not after the first generations
  if (!isFALSE(figurePath) && !requireNamespace("SpaDES.core", quietly = TRUE))
    stop("DEoptim progress plots use SpaDES.core::Plots(); install SpaDES.core or use figurePath = FALSE")
  dots <- list(...)
  objFunArgs <- list(...)
  ## `iterStep` generations run in each DEoptim call, as fireSenseUtils::runDEoptim documents; each call
  ## is one cached chunk, plotted when it finishes. DEoptim adapts CR and F (when c > 0) only within a
  ## call -- it resets them at the start of every call -- so a longer step keeps more of that
  ## adaptation, and a shorter one loses less work to an outage. It is not an objective-function argument.
  iterStep <- if (is.null(dots$iterStep)) 1L else max(1L, as.integer(dots$iterStep))
  objFunArgs$iterStep <- NULL
  ## integers whatever type itermax has: the chunk length is in the cache key, and 2 and 2L digest differently
  chunkEnds <- as.integer(unique(pmin(seq_len(ceiling(control$itermax / iterStep)) * iterStep, control$itermax)))
  chunkLengths <- diff(c(0L, chunkEnds))
  itersToDo <- seq_along(chunkEnds)
  if (is.null(dots$rep)) {
    dots$rep <- runName
  }

  ## Defaults only for what the caller did not set: NP is the caller's (the number of workers the
  ## cluster was built with) and so is strategy. Merging the other way replaced both, so NP was
  ## always 10 x parameters and strategy always 3.
  a <- list(VTR = -Inf, strategy = 3L, NP = NA, itermax = 1, CR = 0.5,
            F = 0.8, bs = FALSE, trace = 1, initialpop = NULL, storepopfrom = 2,
            storepopfreq = 1, p = 0.2, c = 0.5, reltol = 0.1, steptol = 500,
            parallelType = "none", packages = NULL, parVar = NULL, foreachArgs = list(),
            parallelArgs = NULL)

  control <- modifyList(a, as.list(control))

  opts <- options("reproducible.showSimilar" = FALSE)
  on.exit(options(opts), add = TRUE)

  ## The objective function and its arguments are the same in every generation: digest them once
  ## here, instead of Cache() digesting them again in each generation.
  fixedDigest <- reproducible::.robustDigest(list(formals(fn), body(fn), objFunArgs))
  known <- list(keys = NULL, vals = NULL)

  cacheIds <- lapply(seq(itersToDo), function(x) NULL)
  if (FALSE) {
    prevRun <- 1
    sc1 <- showCache(userTags = "DEoptimForCache_1_", after = "2025-05-01 16:40:00", before = "2025-05-05 08:17:00")
    sc2 <- sc1[tagKey == "function"] |> setorderv(cols = "createdDate", order = 1L)
    prevRunCacheIds <- sc2$cacheId
    cacheIds <- c(as.list(prevRunCacheIds), lapply(1:500, function(x) NULL))
  }

  for (iter in itersToDo) {

    control$itermax <- chunkLengths[iter]
    ## no intermediate populations: nothing here uses them, and DEoptim fails indexing them
    ## ("attempt to select less than one element") when storepopfrom is inside a short chunk
    control$storepopfrom <- control$itermax + 1L
    controlForCache <- controlForCache(control)
    chunkStarted <- proc.time()[["elapsed"]]

    if (FALSE) { # for interactive use
      fn(apply(cbind(lower, upper), 1, mean),
         quotedSpread = list(...)$quotedSpread, objFunInner = objFunInner, ...)
    }
    if (TRUE) {
      if (Require:::isRstudio() && FALSE) {
        fn(apply(do.call(rbind, list(lower, upper)), 2, function(x)
          runif(1, min = x[1], max = x[2])), ... )
      }
      DE[[iter]] <- reproducible::Cache(
        .DEoptimChunk,
        fn = fn,
        lower = lower,
        upper = upper,
        control = control,
        known = known,
        dotsList = objFunArgs,
        ## fn and its arguments are in fixedDigest; initialpop, NP and strategy in controlForCache;
        ## the chunk's length is not in controlForCache, and chunks of different lengths differ
        omitArgs = c("fn", "control", "known", "dotsList"),
        .cacheExtra = list(controlForCache, fixedDigest, iter, chunkLengths[iter]),
        cacheId = cacheIds[[iter]],
        .functionName = paste0("DEoptimForCache_", runName, "_", iter),
        ## Every generation is cached even when nested caching is turned off (as
        ## spades.useCache = "eventsOnly" does), so a stopped fit resumes where it was.
        useCache = getOption("clusters.cacheDEoptimIterations", TRUE),
        verbose = .verbose
      )
      ## Was this generation computed in this session, or replayed from the cache? isUpdated() alone
      ## cannot tell when the per-generation cache is off: it is FALSE for a skipped Cache() too.
      computedNow <- !isTRUE(getOption("clusters.cacheDEoptimIterations", TRUE)) ||
        reproducible::isUpdated(DE[[iter]])
      if (!computedNow)
        message(paste(round(unname(DE[[iter]]$optim$bestmem), 4), collapse = " "))
      firstGeneration <- chunkEnds[iter] - chunkLengths[iter] + 1L
      message(cli::col_green(if (chunkLengths[iter] == 1L) paste0("Iteration ", chunkEnds[iter], " done!")
                             else paste0("Iterations ", firstGeneration, "-", chunkEnds[iter], " done!")))
      if (computedNow && !is.null(DE[[iter]]$member$evaluations))
        message(.evaluationTimes(DE[[iter]]$member$evaluations, chunkLengths[iter],
                                 proc.time()[["elapsed"]] - chunkStarted))
    } else {
      # This is for testing --> it is fast
      # fn <- function(par, x) {
      #   -sum(dnorm(log = TRUE, x, mean = par[1], sd = par[2]))
      # }
      #
      # st1 <- system.time(DE[[iter]] <- Cache(DEoptimForCache,
      #                                        fn,
      #                                        lower = lower,
      #                                        upper = upper,
      #                                        mutuallyExclusive = mutuallyExclusive,
      #                                        controlForCache = controlForCache,
      #                                        control = control,
      #                                        omitArgs = c("verbose", "control"),
      #                                        x = x1
      # ))
    }

    control$initialpop <- DE[[iter]]$member$pop
    ## the next generation starts from this population, whose values are already known
    popval <- DE[[iter]]$member$popval
    known <- if (length(popval) == NROW(control$initialpop)) {
      ok <- !is.na(popval)
      list(keys = apply(control$initialpop, 1, .parKey)[ok], vals = popval[ok])
    } else {
      list(keys = NULL, vals = NULL)    # e.g. a generation cached before values were kept
    }

    # if (iter > 499) browser()
    # if (Require:::isRstudio()) if (iter > 200) browser()
    rng <- 25; # how often to do this: i.e., num new iterations per fit, i.e., 1:100, 26:125
    dataRunToUse <- 200 # this will do the lm on this many items
    ## generations so far, over chunks of any length
    bestvalitAll <- unlist(lapply(DE, function(x) x$member$bestvalit))
    nGenerations <- length(bestvalitAll)
    numSegments <- (nGenerations - dataRunToUse) / rng + 1
    numSegmentsBefore <- (nGenerations - chunkLengths[iter] - dataRunToUse) / rng + 1
    pvals <- c(0,0)


    # Do these here because we need them for both sections below
    dfForGGplotSimple <- DEoptimToDataFrame(DE)
    gg1 <- ggPlotFnSimple(dfForGGplotSimple)

    if (numSegments > 1) {
      isNewSegment <- floor(numSegments) > floor(numSegmentsBefore)   # a segment ended in this chunk
      if (isNewSegment) {
        pvals <- numeric(floor(numSegments))
        iters <- list()
        summ <- list()
        # l <- list()
        segmentSeq <- seq_len(floor(numSegments))
        # if (!exists("dfForGGplotSimple", inherits = FALSE))
        DEoutBestValit <- bestvalitAll
        if (!all(is.infinite(DEoutBestValit))) {

          for (i in segmentSeq) {
            col <- "black"
            if (i == tail(segmentSeq, 2)[1]) col <- "blue"
            if (i == tail(segmentSeq, 1)[1]) col <- "red"
            iters[[i]] <- seq_len(dataRunToUse) + (i-1) * rng;
            # message(cli::col_yellow(paste(range(iters), collapse = ":")));
            a <- data.table(iter = seq_len(nGenerations), val = DEoutBestValit)
            lmOut <- try(lm(val ~ iter, data = a[iters[[i]]]))
            if (!is(lmOut, "try-error")) {
              # next
              summ[[i]] <- summary(lmOut);
              pvals[i] <- round(summ[[i]]$coefficients[2, 4], 4)

              newdat <- data.table(iter = iters[[i]])
              set(newdat, NULL, "pred", predict(lmOut, newdata = newdat))
              int <- summ[[i]]$coefficients[1, 1]
              slop <- summ[[i]]$coefficients[2, 1]
              # gg1 <- gg1 + geom_line(data = newdat,
              #                           aes(x = iter, y = pred), #, xend = tail(iter, 1), yend = tail(pred, 1)),
              #                           col = col)
              gg1 <- gg1 + geom_abline(intercept = int, slope = slop,
                                       #                        aes(x = iter, y = pred), #, xend = tail(iter, 1), yend = tail(pred, 1)),
                                       col = col)
            }
          }
          pvalDT <- data.table(dataRange = sapply(segmentSeq, function(x) paste(range(iters[[x]]), collapse = ":")),
                               pvals = pvals)
          reproducible::messageDF(pvalDT, colour = "yellow")
        }

      }
    }
    # Break out if the last N segments are "non-significant slope at p == 0.1 i.e., conservative
    converged <- all(tail(pvals, 2) > 0.1) && nGenerations > 349
    ## Plot chunks computed in this session, not ones replayed from the cache, when each chunk (`iterStep`
    ## generations) finishes. Plotting used to require reproducible::isUpdated(), which is
    ## FALSE whenever nested Cache() is skipped (spades.useCache = "eventsOnly"), so no progress plots
    ## were made at all (FireSense phase 2, 2026-09-15; the 2026-09-08 fits still had them).
    if (!isFALSE(figurePath) && computedNow) { # i.e., should be a path
      message(cli::col_green("Plotting DEoptim progress at iteration ", chunkEnds[iter], " (every ", iterStep,
                             " iterations) to ", figurePath))
      if (!is.null(dots$formulaToFit))
        terms <- suppressMessages(termsInDEoptim(dots$formulaToFit, dots$thresh, length(lower)))
      else
        terms <- names(lower)
      nVars <- NCOL(DE[[iter]]$member$pop)
      if (length(terms) != nVars )
        terms <- c(terms, paste0("V", seq(nVars - length(terms))))
      dfForGGplot <- visualizeDEoptimLines(DE, terms = terms)
      dfForGGplotAllPoints <- visualizeDEoptimLines(DE, terms = terms, allPoints = TRUE)
      dfForGGplotSimple <- DEoptimToDataFrame(DE)


      texts <- c("objFun/", "lines_mean_AllPoints/", "lines_mean/", "lines_dif/", "lines_variance/", "hists/")
      withCallingHandlers({
        SpaDES.core::Plots(gg1, types = .plots,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[1]))
        SpaDES.core::Plots(dfForGGplotAllPoints, ggPlotFnMeansAllPoints, types = .plots,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[2]));
        SpaDES.core::Plots(dfForGGplot, ggPlotFnMeans, types = .plots,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[3]))
        SpaDES.core::Plots(dfForGGplot, ggPlotFnDif, types = .plots, ,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[4]))
        SpaDES.core::Plots(dfForGGplot, ggPlotFnVars, types = .plots, ,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[5]))
        SpaDES.core::Plots(visualizeDE(DE = DE[[iter]], cachePath = cachePath,
              titles = terms, lower = lower, upper = upper), types = .plots,
              filename = ggDEoptimFilename(figurePath, rep = dots$rep, subfolder = "", iter = chunkEnds[iter], text = texts[6], time = TRUE))
      }, message = function(m) {
        if (any(grepl("geom_smooth|Saving", m$message)))
          invokeRestart("muffleMessage")
        reproducible::messageColoured(colour = "green", gsub("Saved figure to: ", "Saved: ", m$message))
        invokeRestart("muffleMessage")
      })
      # reproducible::messageColoured(colour = "green",
      #                               paste0(length(texts), " figures saved to: ",
      #                               dirname(ggDEoptimFilename(figurePath, rep, subfolder = "", text = texts))),
      #                               verbose = .verbose)

    }


    # Break out if the last N segments are "non-significant slope at p == 0.1 i.e., conservative
    if (converged) {
      break
    }
  }
  DE
}


controlSet <- function(control, ...) {
  if (length(names(control)) < 20)
    control <- do.call("DEoptim.control", control)
  missingElements <- ...names() %in% names(control)
  if (any(missingElements)) {
    control <- Require::modifyList2(control, list(...))

    # control$itermax <- pmin(iterStep, itermax - iterStep * (iter - 1))
    # control$storepopfrom <- control$itermax + 1
    # control$reltol <- 0.1
    # control$c <- .c
    #
  }
  control

}

controlForCache <- function(controlArgs) {
  controlArgs[intersect(names(controlArgs), c(
    "VTR", "strategy", "NP", "CR", "F", "bs", # "trace",
    "initialpop", "p", "c", "reltol",
    "packages", "parVar", "foreachArgs"
  ))]
}


# @importFrom DEoptim DEoptim
# DEoptimForCache <- function(...) {
#   dots <- list(...)
#   dots["controlForCache"] <- NULL
#   do.call(DEoptim, dots)
# }


ggPlotFnMeans <- function(bmerged) {
  ggplot(bmerged, aes(iter, value)) +
    geom_point() +
    geom_smooth(se = TRUE) +
    # geom_ribbon(aes(ymin = lower95, ymax = upper95)) +
    facet_wrap(facets = "variable", scales = "free")
}

ggPlotFnSimple <- function(bmerged) {
  ggplot(bmerged, aes(iter, bestValue)) +
    geom_point() +
    geom_smooth(se = TRUE)
}

ggPlotFnDif <- function(bmerged) {
  ggplot(bmerged, aes(iter, dif)) +
    geom_point() +
    geom_smooth(se = TRUE) +
    # geom_ribbon(aes(ymin = lower95, ymax = upper95)) +
    facet_wrap(facets = "variable", scales = "free")
}

ggPlotFnVars <- function(bmerged) {
  ggplot(bmerged, aes(iter, var)) +
    geom_point() +
    geom_smooth(se = TRUE) +
    # geom_ribbon(aes(ymin = lower95, ymax = upper95)) +
    facet_wrap(facets = "variable", scales = "free")
}


ggPlotFnMeansAllPoints <- function(b) {
  ggplot(b, aes(iter, value)) +
    # geom_point() +
    geom_jitter(size = 0.05, width = 0.2, col = "grey") +
    geom_smooth(se = TRUE) +
    # geom_ribbon(aes(ymin = lower95, ymax = upper95)) +
    facet_wrap(facets = "variable", scales = "free")
}



ggDEoptimFilename <- function(visualizeDEoptim, rep, iter = NULL, subfolder = "fireSense_SpreadFit",
                              text = "DE_hists_", time = FALSE) {
  if (is.numeric(rep))
    rep <- reproducible::paddedFloatToChar(rep, padL = 3)
  file.path(visualizeDEoptim,
            subfolder,
            paste0(text, rep,
                   ifelse(is.null(iter), "", paste0("_iter", iter)), "_", Sys.getpid(),
                   ifelse(isTRUE(time), paste0("_", as.character(round(Sys.time(), 0))), ""), ".png")) |>
    normalizePath(winslash = "/", mustWork = FALSE)
}


#' Make histograms of `DEoptim` object `pars`
#'
#' @param DE An object from a `DEoptim` call
#' @param cachePath A `cacheRepo` to pass to `showCache` and
#'        `loadFromCache` if `DE` is missing.
#'
#' @param titles Character vector of parameter names, one per histogram.
#' @param lower Named numeric of lower bounds, used to fix each panel's x range.
#' @param upper Named numeric of upper bounds, used to fix each panel's x range.
#' @export
#' @importFrom data.table := as.data.table melt set setnames
#' @importFrom graphics hist par
#' @importFrom reproducible loadFromCache showCache
#' @importFrom utils tail
#' @import ggplot2
visualizeDE <- function(DE, cachePath, titles, lower, upper) {
  if (missing(DE)) {
    if (missing(cachePath)) {
      stop("Must provide either DE or cachePath")
    }
    message("DE not supplied; visualizing the most recent added to Cache")
    sc <- showCache(userTags = "DEoptim")
    cacheID <- tail(sc$cacheId, 1)
    DE <- reproducible::loadFromCache(cachePath, cacheId = cacheID)
  }
  if (is(DE, "list")) {
    DE <- tail(DE, 1)[[1]]
  }

  cc <- as.data.table(DE$member$pop)
  setnames(cc, titles)
  suppressWarnings(bb <- melt(cc))
  ff <- lapply(titles, function(p) {
    ggplot(bb[variable == p], aes(value)) +
      geom_histogram(bins = 15) + coord_cartesian(xlim = c(lower[p],upper[p])) +
      ggtitle(p) + xlab(NULL) +
      theme_minimal()
  })
  ## Suggests, not Imports: one ggarrange call does not justify pulling ggpubr's
  ## dependency tree into every install, CI runs included.
  if (!requireNamespace("ggpubr", quietly = TRUE)) {
    message("Install ggpubr to arrange these ", length(ff), " plots on one page; ",
            "returning the list instead.")
    return(invisible(ff))
  }
  invisible(ggpubr::ggarrange(plotlist = ff))
}




#' `termsInDEoptim`
#'
#' @param fireSense_spreadFormula The formula to be submitted to [DEoptim::DEoptim()],
#'                                from e.g., `sim$fireSense_spreadFormula`.
#'
#' @param thresh The threshold for accepting fits; e.g., from `mod$thresh`.
#'
#' @param numParams The number of parameters (TODO: improve description)
#'
#' @export
#' @rdname runDEoptim
termsInDEoptim <- function(fireSense_spreadFormula, thresh, numParams) {
  termsInForm <- attr(terms(as.formula(fireSense_spreadFormula, env = .GlobalEnv)), "term.labels")
  logitNumParams <- numParams - length(termsInForm)
  message("Using a ", logitNumParams, " parameter logistic equation")
  message("  There will be ", logitNumParams, " logit terms & ", numParams, " terms in all:")
  message("  ", paste(c(paste0("logit", seq(logitNumParams)), termsInForm), collapse = ", "))
  message("  objectiveFunction threshold SNLL to run all years after first 2 years: ", thresh)
  c(paste0("logit", seq(logitNumParams)), termsInForm)
}


DEoptimToDataFrame <- function(d, item = "bestvalit") {
  b <- lapply(d, function(dr) as.data.frame(dr$member[[item]]) |> setNames("bestValue"))
  b <- rbindlist(b, idcol = "iter")
  b[, iter := seq_len(.N)]   # one value per generation, over chunks of any length
  b
}

visualizeDEoptimLines <- function(d, terms, allPoints = FALSE) {
  ## the generation at the end of each chunk (1, 2, 3, ... when each chunk is one generation)
  se <- cumsum(vapply(d, function(dr) length(dr$member$bestvalit), integer(1)))
  # this commented code will use "all the population
  if (isTRUE(allPoints)) {
    b <- lapply(d, function(dr) as.data.frame(dr$member$pop))
    b <- rbindlist(b, idcol = "iter")
    b[, iter := se[iter]]
    setnames(b, old = grep("^V", colnames(b), value = TRUE),  terms)

  } else {
    b <- do.call(rbind, lapply(d, function(dr) colMeans(dr$member$pop))) |> as.data.table()
    setnames(b, terms)
    blower <- do.call(rbind, lapply(d, function(dr)
      sapply(seq(NCOL(dr$member$pop)), function(x) quantile(dr$member$pop[, x], 0.025)))) |>
      as.data.table()
    bupper <- do.call(rbind, lapply(d, function(dr)
      sapply(seq(NCOL(dr$member$pop)), function(x) quantile(dr$member$pop[, x], 0.975)))) |>
      as.data.table()
    bvar <- do.call(rbind, lapply(d, function(dr)
      sapply(seq(NCOL(dr$member$pop)), function(x) var(dr$member$pop[, x])))) |>
      as.data.table()
    setnames(blower, names(b))
    setnames(bupper, names(b))
    setnames(bvar, names(b))
    b[, iter := se]
    blower[, iter := se]
    bupper[, iter := se]
    bvar[, iter := se]
    blower <- melt(blower, id.vars = "iter")
    setnames(blower, old = "value", new = "lower95")
    bupper <- melt(bupper, id.vars = "iter")
    setnames(bupper, old = "value", new = "upper95")
    bvar <- melt(bvar, id.vars = "iter")
    setnames(bvar, old = "value", new = "var")


  }

  b <- melt(b, id.vars = "iter")
  if (isTRUE(allPoints)) {
    bmerged <- b
  } else {
    ons <- c("iter", "variable")
    bmerged <- b[blower, on = ons][bupper, on = ons][bvar, on = ons]
    bmerged[, dif := upper95 - lower95]
  }
  bmerged[]
}


ggPlotFnMeans <- function(bmerged) {
  ggplot(bmerged, aes(iter, value)) +
    geom_point() +
    geom_smooth(se = TRUE) +
    # geom_ribbon(aes(ymin = lower95, ymax = upper95)) +
    facet_wrap(facets = "variable", scales = "free")
}

ggPlotFnSimple <- function(bmerged) {
  ggplot(bmerged, aes(iter, bestValue)) +
    geom_point() +
    geom_smooth(se = TRUE)
}

ggPlotFnDif <- function(bmerged) {
  ggplot(bmerged, aes(iter, dif)) +
    geom_point() +
    geom_smooth(se = TRUE) +
    # geom_ribbon(aes(ymin = lower95, ymax = upper95)) +
    facet_wrap(facets = "variable", scales = "free")
}

ggPlotFnVars <- function(bmerged) {
  ggplot(bmerged, aes(iter, var)) +
    geom_point() +
    geom_smooth(se = TRUE) +
    # geom_ribbon(aes(ymin = lower95, ymax = upper95)) +
    facet_wrap(facets = "variable", scales = "free")
}


ggPlotFnMeansAllPoints <- function(b) {
  ggplot(b, aes(iter, value)) +
    # geom_point() +
    geom_jitter(size = 0.05, width = 0.2, col = "grey") +
    geom_smooth(se = TRUE) +
    # geom_ribbon(aes(ymin = lower95, ymax = upper95)) +
    facet_wrap(facets = "variable", scales = "free")
}



# ggDEoptimFilename <- function(visualizeDEoptim, rep, iter = NULL, text = "DE_hists_", time = FALSE) {
#   if (is.numeric(rep))
#     rep <- paddedFloatToChar(rep, padL = 3)
#   file.path(visualizeDEoptim,
#             "fireSense_SpreadFit",
#             paste0(text, "rep", rep,
#                    ifelse(is.null(iter), "", paste0("_iter", iter)), "_", Sys.getpid(),
#                    ifelse(isTRUE(time), paste0("_", as.character(round(Sys.time(), 0))), ""), ".png"))
# }

