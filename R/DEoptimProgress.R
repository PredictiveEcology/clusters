## DEoptim progress files: DEoptimIterative() writes one small csv per fit; DEoptimProgress() reads them
## and DEoptimDashboard() shows them. A fit's file is long-format, one row per variable and chunk:
## time, generation, variable ("objective", a parameter's name, or "FINISHED"), best, q10, median, q90.

.progressObjective <- "objective"    # the `variable` of the objective's rows
.progressFinished <- "FINISHED"      # the `variable` of the row that ends a file, and a fit's status
.progressColumns <- c("time", "generation", "variable", "best", "q10", "median", "q90")

## The file a fit writes: `progressFile` (NULL: in `figurePath`, or "." without one; FALSE: none), emptied
## to its header. Returns the path, or FALSE.
.progressStart <- function(progressFile, figurePath, runName) {
  if (isFALSE(progressFile)) return(FALSE)
  if (is.null(progressFile))
    progressFile <- file.path(if (isFALSE(figurePath)) "." else figurePath,
                              paste0("DEoptimProgress_", runName, ".csv"))
  .progressWrite(progressFile, data.table::as.data.table(
    stats::setNames(rep(list(character()), length(.progressColumns)), .progressColumns)), append = FALSE)
  progressFile
}

## Rows for the chunk ending at `generation`: the objective, then each parameter (best member's value and
## the population's quantiles). `finished`: one FINISHED row instead.
.progressRows <- function(member, generation, parNames, finished = FALSE) {
  row <- function(variable, best, x) data.frame(
    time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"), generation = generation, variable = variable, best = best,
    q10 = if (is.null(x)) NA_real_ else stats::quantile(x, 0.1, names = FALSE),
    median = if (is.null(x)) NA_real_ else stats::median(x),
    q90 = if (is.null(x)) NA_real_ else stats::quantile(x, 0.9, names = FALSE))
  if (finished) return(row(.progressFinished, NA_real_, NULL))
  pop <- member$pop
  bestRow <- which.min(member$popval)
  if (length(parNames) != NCOL(pop)) parNames <- paste0("V", seq_len(NCOL(pop)))
  do.call(rbind, c(list(row(.progressObjective, min(member$popval), member$popval)),
                   lapply(seq_len(NCOL(pop)), function(j) row(parNames[j], pop[bestRow, j], pop[, j]))))
}

.progressWrite <- function(file, rows, append) {
  tryCatch({
    dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
    data.table::fwrite(rows, file, append = append)
  }, error = function(e) warning("could not write the DEoptim progress file ", file, ": ", conditionMessage(e),
                                 call. = FALSE))
}

## Append the rows of a chunk (or the FINISHED row) to a fit's file; a no-op when `file` is FALSE.
.progressAppend <- function(file, member, generation, parNames, finished = FALSE) {
  if (isFALSE(file)) return(invisible())
  tryCatch(.progressWrite(file, .progressRows(member, generation, parNames, finished), append = TRUE),
           error = function(e) warning("could not make the DEoptim progress rows: ", conditionMessage(e),
                                       call. = FALSE))
  invisible()
}

#' Read the progress files of DEoptim fits
#'
#' Reads the files [DEoptimIterative()] writes (`progressFile`), found recursively under `path`.
#' A file is read again only when its modification time has changed since the last call with the same `state`.
#'
#' @param path Folder searched recursively.
#' @param pattern Regular expression the file names must match.
#' @param runningWithin Seconds. A fit without a `FINISHED` row is `RUNNING` when its file changed within
#'   this time, and `STOPPED` otherwise.
#' @param state An environment that keeps what has been read, so a refresh reads only changed files.
#'
#' @return A list of three data.tables. `fits`: one row per file (`fit` the file, `label` its folder
#'   relative to `path` plus the run name when it is not `1`, `status`, `generation`, `best` objective,
#'   `started`, `updated`). `gens`: `fit`, `generation`, `best`, `q10`, `median` of the objective.
#'   `params`: `fit`, `generation`, `param`, `best`, `q10`, `median`, `q90`.
#' @export
DEoptimProgress <- function(path = ".", pattern = "^DEoptimProgress_.*\\.csv$", runningWithin = 15 * 60,
                            state = new.env()) {
  files <- list.files(path, pattern = pattern, recursive = TRUE, full.names = TRUE)
  if (is.null(state$files)) state$files <- list()
  mtime <- file.mtime(files)
  for (i in seq_along(files)) {
    old <- state$files[[files[i]]]
    if (is.null(old) || !identical(old$mtime, mtime[i]))
      state$files[[files[i]]] <- list(mtime = mtime[i], data = .progressRead(files[i]))
  }
  files <- files[vapply(state$files[files], function(x) !is.null(x$data), logical(1))]
  empty <- list(fits = data.table::data.table(), gens = data.table::data.table(),
                params = data.table::data.table())
  if (!length(files)) return(empty)
  fits <- data.table::rbindlist(lapply(files, function(f) {
    d <- state$files[[f]]$data
    obj <- d[d$variable == .progressObjective, ]
    runName <- sub("\\.csv$", "", sub("^DEoptimProgress_", "", basename(f)))
    folder <- sub("^\\.$", "", dirname(.relativeTo(f, path)))
    data.table::data.table(
      fit = f,
      label = paste(c(if (nzchar(folder)) folder, if (runName != "1") runName), collapse = " "),
      status = if (any(d$variable == .progressFinished)) .progressFinished
               else if (difftime(Sys.time(), state$files[[f]]$mtime, units = "secs") <= runningWithin) "RUNNING"
               else "STOPPED",
      generation = if (nrow(obj)) max(obj$generation) else NA_integer_,
      best = if (nrow(obj)) obj$best[which.max(obj$generation)] else NA_real_,
      started = d$time[1], updated = state$files[[f]]$mtime)
  }))
  fits[fits$label == "", "label" := "."]
  parts <- lapply(files, function(f) cbind(fit = f, state$files[[f]]$data))
  rows <- data.table::rbindlist(parts)
  obj <- rows[rows$variable == .progressObjective, ]
  par <- rows[!rows$variable %in% c(.progressObjective, .progressFinished), ]
  list(fits = fits,
       gens = obj[, c("fit", "generation", "best", "q10", "median")],
       params = data.table::setnames(par[, c("fit", "generation", "variable", "best", "q10", "median", "q90")],
                                     "variable", "param"))
}

.progressRead <- function(file) {
  d <- tryCatch(data.table::fread(file, colClasses = list(character = c("time", "variable"))),
                error = function(e) NULL)
  if (is.null(d) || !nrow(d) || !all(.progressColumns %in% names(d))) return(NULL)
  d$time <- as.POSIXct(d$time, format = "%Y-%m-%dT%H:%M:%S")
  d
}

## `file` relative to the folder `path`
.relativeTo <- function(file, path) {
  file <- normalizePath(file, mustWork = FALSE)
  path <- normalizePath(path, mustWork = FALSE)
  sub(paste0("^", gsub("([][{}()+*^$|\\\\?.])", "\\\\\\1", path), "/?"), "", file)
}

#' A Shiny dashboard of DEoptim fits
#'
#' A table of the fits whose progress files ([DEoptimProgress()]) are under `path`, then a card per fit with
#' its objective by generation and, in a collapsed section, its parameters by generation. It re-reads the
#' files every `refreshMinutes`. Needs the shiny package.
#'
#' @inheritParams DEoptimProgress
#' @param refreshMinutes Minutes between reads.
#' @param title Title of the page.
#' @param description Text under the title.
#' @param launch.browser Passed to [shiny::runApp()].
#'
#' @return `DEoptimDashboard()` runs the app. `DEoptimDashboardApp()` returns it, as a `shiny::shinyApp`.
#' @export
DEoptimDashboard <- function(path = ".", refreshMinutes = 2, title = "DEoptim fits",
                             description = .dashboardDescription,
                             launch.browser = getOption("shiny.launch.browser", interactive())) {
  shiny::runApp(DEoptimDashboardApp(path, refreshMinutes, title, description), launch.browser = launch.browser)
}

.dashboardDescription <- paste(
  "One card per fit (DEoptim): the objective of the best member, the 10th percentile and the median of the",
  "population at each generation; lower is better. The y axis stops at the 90th percentile, so the first, very",
  "poor generations do not flatten it. Open 'parameters by generation' for each parameter's 10th-90th",
  "percentile band (blue), median (black) and best member (orange). Running: the fit's file changed in the",
  "last 15 min. Finished: the fit is done.")

#' @rdname DEoptimDashboard
#' @export
DEoptimDashboardApp <- function(path = ".", refreshMinutes = 2, title = "DEoptim fits",
                                description = .dashboardDescription) {
  if (!requireNamespace("shiny", quietly = TRUE)) stop("DEoptimDashboard needs the shiny package")
  state <- new.env()
  ui <- shiny::fluidPage(
    ## Bootstrap hides the disclosure triangle of <summary>
    shiny::tags$style("details > summary { display: list-item; cursor: pointer; color: #5d655f; }"),
    shiny::titlePanel(title),
    shiny::p(description),
    shiny::textOutput("checked"),
    shiny::tableOutput("fits"),
    shiny::uiOutput("cards"))
  server <- function(input, output, session) {
    data <- shiny::reactive({
      shiny::invalidateLater(refreshMinutes * 60 * 1000)
      d <- DEoptimProgress(path, state = state)
      d$fits <- d$fits[order(d$fits$label), ]
      d
    })
    output$checked <- shiny::renderText(paste("read", format(Sys.time(), "%H:%M:%S"), "- refreshes every",
                                              refreshMinutes, "min"))
    output$fits <- shiny::renderTable({
      f <- data()$fits
      if (!nrow(f)) return(data.frame(note = "No DEoptim progress files found."))
      data.frame(fit = f$label, status = f$status, generation = f$generation,
                 `best objective` = round(f$best, 1), started = format(f$started, "%m-%d %H:%M"),
                 updated = format(f$updated, "%m-%d %H:%M"), check.names = FALSE)
    })
    ## one card per fit; the parameter plot is drawn when its section is first opened (Shiny does not
    ## draw hidden outputs; "shown" makes it check again)
    output$cards <- shiny::renderUI({
      f <- data()$fits
      shiny::fluidRow(lapply(seq_len(nrow(f)), function(i) shiny::column(
        4, shiny::tags$h4(paste(f$label[i], "-", tolower(f$status[i]), "- generation", f$generation[i])),
        shiny::plotOutput(paste0("obj_", i), height = "260px"),
        shiny::tags$details(ontoggle = "$(this).trigger('shown')",
                            shiny::tags$summary("parameters by generation"),
                            shiny::plotOutput(paste0("par_", i), height = "520px")))))
    })
    ## output ids are the card's position; the fit is looked up when drawn, so a refresh keeps the plots
    shiny::observe({
      for (i in seq_len(nrow(data()$fits))) local({
        i <- i
        ofFit <- function(x) { x <- x[x$fit == data()$fits$fit[i], ]; x[order(x$generation), ] }
        output[[paste0("obj_", i)]] <- shiny::renderPlot(.plotObjective(ofFit(data()$gens)))
        output[[paste0("par_", i)]] <- shiny::renderPlot(.plotParams(ofFit(data()$params)))
      })
    })
  }
  shiny::shinyApp(ui, server)
}

.plotObjective <- function(g) {
  if (!nrow(g)) return(invisible())
  cols <- c("#b5480e", "#1d6fa5", "grey20")
  ## the y range stops at the 90th percentile, so the first, very poor generations do not flatten it
  yl <- c(min(g$best), stats::quantile(c(g$median, g$q10), 0.9))
  graphics::par(mar = c(3, 3.2, 0.5, 0.5), mgp = c(1.9, 0.6, 0))
  graphics::matplot(g$generation, g[, c("best", "q10", "median")], type = "l", lty = c(1, 2, 1),
                    lwd = c(2, 1.2, 1.6), col = cols, ylim = yl, xlab = "generation",
                    ylab = "objective (lower is better)")
  graphics::legend("topright", c("best member", "10th percentile", "median"), lty = c(1, 2, 1),
                   col = cols, bty = "n", cex = 0.8)
}

.plotParams <- function(p) {
  if (!nrow(p)) return(invisible())
  nms <- unique(p$param)
  graphics::par(mfrow = c(ceiling(length(nms) / 2), 2), mar = c(3, 3, 2, 1), mgp = c(1.8, 0.6, 0))
  for (nm in nms) {
    q <- p[p$param == nm, ]
    graphics::plot(q$generation, q$median, type = "n", ylim = range(q$q10, q$q90, q$best), main = nm,
                   xlab = "generation", ylab = "")
    graphics::polygon(c(q$generation, rev(q$generation)), c(q$q10, rev(q$q90)), col = "#1d6fa533", border = NA)
    graphics::lines(q$generation, q$median, col = "grey20", lwd = 1.4)
    graphics::lines(q$generation, q$best, col = "#b5480e", lwd = 1.4)
    if (min(q$q10) < 0 && max(q$q90) > 0) graphics::abline(h = 0, lty = 3, col = "grey50")
  }
}
