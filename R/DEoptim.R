

DEoptimIterative2 <- function(fn, lower, upper, control, ...,
                              # FS_formula, covMinMax, tests, maxFireSpread, mutuallyExclusive,
                              # doObjFunAssertions, Nreps, objFunCoresInternal, thresh, rep,
                              .plots, figurePath, cachePath, runName = 1, .verbose = TRUE) {
  DE <- list()
  dots <- list(...)
  itersToDo <- seq(control$itermax)
  control$itermax <- 1L
  if (is.null(list(...)$rep)) {
    rep <- runName
  }

  control$cluster
  a <- list(VTR = -Inf, strategy = 3L, NP = NA, itermax = 1, CR = 0.5,
            F = 0.8, bs = FALSE, trace = 1, initialpop = NULL, storepopfrom = 2,
            storepopfreq = 1, p = 0.2, c = 0.5, reltol = 0.1, steptol = 500,
            parallelType = "none", packages = NULL, parVar = NULL, foreachArgs = list(),
            parallelArgs = NULL)

  control <- modifyList(control, a)

  opts <- options("reproducible.showSimilar" = FALSE)
  on.exit(options(opts), add = TRUE)
  for (iter in itersToDo) {

    controlForCache <- controlForCache(control)

    if (FALSE) { # for interactive use
      fn(p = apply(cbind(lower, upper), 1, mean),
         quotedSpread = list(...)$quotedSpread, objFunInner = objFunInner)
    }
    if (TRUE) {
      if (Require:::isRstudio()) {
       #  debug(fn)
        fn(par = apply(do.call(rbind, list(lower, upper)), 2, function(x) runif(1, min = x[1], max = x[2])), plot.it = TRUE, ... )
      }
      DE[[iter]] <- Cache(
        DEoptim(
          fn,
          lower = lower,
          upper = upper,
          control = control, ...
          # formulaToFit = formulaToFit,
          # covMinMax = covMinMax,
          # tests = tests,
          # maxFireSpread = maxFireSpread,
          # mutuallyExclusive = mutuallyExclusive,
          # doAssertions = doObjFunAssertions,
          # Nreps = Nreps,
          # plot.it = FALSE,
          # objFunCoresInternal = objFunCoresInternal,
          # thresh = thresh
        ),
        .cacheExtra = list(controlForCache, runName, iter),
        # cacheId = cacheIds[iter],
        .functionName = paste0("DEoptimForCache_", runName, "_", iter),
        verbose = .verbose,
        omitArgs = c("verbose", "control")
      )
      if (!isUpdated(DE[[iter]]))
        message(paste(round(unname(DE[[iter]]$optim$bestmem), 4), collapse = " "))
      message(cli::col_green("Iteration ", iter, " done!"))
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

    rng <- 5; # do X iteration steps, i.e., 1:100, 6:125
    dataRunToUse <- 50 # this will do the lm on this many items
    numSegments <- (length(DE) - dataRunToUse) / rng + 1# (length(DE) - dataRunToUse + 1) / rng
    pvals <- c(0,0)


    # Do these here because we need them for both sections below
    dfForGGplotSimple <- DEoptimToDataFrame(DE)
    gg1 <- ggPlotFnSimple(dfForGGplotSimple)

    if (numSegments > 1) {
      isNewSegment <- numSegments %% 1 == 0
      if (isNewSegment) {
        pvals <- numeric(floor(numSegments))
        iters <- list()
        summ <- list()
        # l <- list()
        segmentSeq <- seq_len(floor(numSegments))
        # if (!exists("dfForGGplotSimple", inherits = FALSE))
        DEoutBestValit <- sapply(DE, function(x) x$member$bestvalit)
        if (!all(is.infinite(DEoutBestValit))) {

          for (i in segmentSeq) {
            col <- "black"
            if (i == tail(segmentSeq, 2)[1]) col <- "blue"
            if (i == tail(segmentSeq, 1)[1]) col <- "red"
            iters[[i]] <- seq_len(dataRunToUse) + (i-1) * rng;
            # message(cli::col_yellow(paste(range(iters), collapse = ":")));
            a <- data.table(iter = seq_along(DE), val = DEoutBestValit)
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
    if (!isFALSE(figurePath) && (isUpdated(DE[[iter]]))) { # i.e., should be a path
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
        Plots(gg1, types = .plots,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[1]))
        Plots(dfForGGplotAllPoints, ggPlotFnMeansAllPoints, types = .plots,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[2]));
        Plots(dfForGGplot, ggPlotFnMeans, types = .plots,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[3]))
        Plots(dfForGGplot, ggPlotFnDif, types = .plots, ,
              filename = ggDEoptimFilename(figurePath, dots$rep, subfolder = "", text = texts[4]))
        Plots(dfForGGplot, ggPlotFnVars, types = .plots, ,
              filename = ggDEoptimFilename(figurePath, rep, subfolder = "", text = texts[5]))
        Plots(fn = visualizeDE, DE = DE[[iter]], cachePath = cachePath,
              titles = terms, lower = lower, upper = upper, types = .plots,
              filename = ggDEoptimFilename(figurePath, rep = rep, subfolder = "", iter = iter, text = texts[6], time = TRUE))
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
    if (all(tail(pvals, 2) > 0.1) && length(DE) > 349) {
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
    control <- modifyList2(control, list(...))

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
    rep <- paddedFloatToChar(rep, padL = 3)
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
#' @export
#' @importFrom data.table as.data.table
#' @importFrom graphics hist par
#' @importFrom reproducible loadFromCache showCache
#' @importFrom utils tail
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
  b
}

visualizeDEoptimLines <- function(d, terms, allPoints = FALSE) {
  iter <- length(d)
  se <- seq(iter)
  # this commented code will use "all the population
  if (isTRUE(allPoints)) {
    b <- lapply(d, function(dr) as.data.frame(dr$member$pop))
    b <- rbindlist(b, idcol = "iter")#
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

