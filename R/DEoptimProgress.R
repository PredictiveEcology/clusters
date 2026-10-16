## DEoptim progress files: DEoptimIterative() writes one small csv per fit; DEoptimProgress() reads them
## and DEoptimDashboard() shows them. A fit's file is long-format, one row per variable and chunk:
## time, generation, variable ("objective", a parameter's name, or "FINISHED"), best, q10, median, q90.

.progressObjective <- "objective"    # the `variable` of the objective's rows
.progressFinished <- "FINISHED"      # the `variable` of the row that ends a file, and a fit's status
.progressErrored <- "ERRORED"        # the same, for a fit that stopped with an R error
.progressMarkers <- c(.progressFinished, .progressErrored)
.progressStatuses <- c("running", "stopped", "finished", "errored")   # the values of `which`
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
## the population's quantiles). `marker` (.progressFinished or .progressErrored): one row of it instead.
.progressRows <- function(member, generation, parNames, marker = NULL) {
  row <- function(variable, best, x) data.frame(
    time = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"), generation = generation, variable = variable, best = best,
    q10 = if (is.null(x)) NA_real_ else stats::quantile(x, 0.1, names = FALSE),
    median = if (is.null(x)) NA_real_ else stats::median(x),
    q90 = if (is.null(x)) NA_real_ else stats::quantile(x, 0.9, names = FALSE))
  if (!is.null(marker)) return(row(marker, NA_real_, NULL))
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

## Append the rows of a chunk (or an end marker row) to a fit's file; a no-op when `file` is FALSE.
.progressAppend <- function(file, member, generation, parNames, marker = NULL) {
  if (isFALSE(file)) return(invisible())
  tryCatch(.progressWrite(file, .progressRows(member, generation, parNames, marker), append = TRUE),
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
#' @param runningWithin Seconds. A fit without a `FINISHED` or `ERRORED` row is `RUNNING` when its file
#'   changed within this time, and `STOPPED` otherwise (a killed process leaves no marker).
#' @param which Which fits to return: any of `"running"`, `"stopped"`, `"finished"`, `"errored"`, in any case.
#'   The default is all of them. `gens` and `params` are cut to the same fits.
#' @param state An environment that keeps what has been read, so a refresh reads only changed files.
#'
#' @return A list of three data.tables. `fits`: one row per file (`fit` the file, `label` its folder
#'   relative to `path` plus the run name when it is not `1`, `status`, `generation`, `best` objective,
#'   `started`, `updated`). `gens`: `fit`, `generation`, `best`, `q10`, `median` of the objective.
#'   `params`: `fit`, `generation`, `param`, `best`, `q10`, `median`, `q90`.
#' @export
DEoptimProgress <- function(path = ".", pattern = "^DEoptimProgress_.*\\.csv$", runningWithin = 15 * 60,
                            state = new.env(), which = .progressStatuses) {
  which <- toupper(match.arg(tolower(which), .progressStatuses, several.ok = TRUE))
  files <- list.files(path, pattern = pattern, recursive = TRUE, full.names = TRUE)
  ## a folder of symlinks to fits under `path` lists each file twice; the label is the real location's
  files <- files[!duplicated(normalizePath(files, mustWork = FALSE))]
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
               else if (any(d$variable == .progressErrored)) .progressErrored
               else if (difftime(Sys.time(), state$files[[f]]$mtime, units = "secs") <= runningWithin) "RUNNING"
               else "STOPPED",
      generation = if (nrow(obj)) max(obj$generation) else NA_integer_,
      best = if (nrow(obj)) obj$best[which.max(obj$generation)] else NA_real_,
      started = d$time[1], updated = state$files[[f]]$mtime)
  }))
  fits[fits$label == "", "label" := "."]
  fits <- fits[fits$status %in% which, ]
  files <- fits$fit
  if (!length(files)) return(empty)
  parts <- lapply(files, function(f) cbind(fit = f, state$files[[f]]$data))
  rows <- data.table::rbindlist(parts)
  obj <- rows[rows$variable == .progressObjective, ]
  par <- rows[!rows$variable %in% c(.progressObjective, .progressMarkers), ]
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

## `file` relative to the folder `path`. Both with "/" as the separator: on Windows normalizePath()
## gives "\\" by default, which the "/" after the folder did not match (CI, 2026-10-08: labels "/ELF1").
.relativeTo <- function(file, path) {
  file <- normalizePath(file, winslash = "/", mustWork = FALSE)
  path <- sub("/+$", "", normalizePath(path, winslash = "/", mustWork = FALSE))
  inside <- startsWith(file, paste0(path, "/"))
  file[inside] <- substring(file[inside], nchar(path) + 2L)
  file
}

#' A Shiny dashboard of DEoptim fits
#'
#' A table of the fits whose progress files ([DEoptimProgress()]) are under `path`, then a card per fit with
#' its objective by generation and, in a collapsed section, its parameters by generation. It re-reads the
#' files every `refreshMinutes`. Click a column header of the table to sort by it; the sort order, page and
#' search are kept when the table refreshes. Needs the shiny and DT packages.
#'
#' @inheritParams DEoptimProgress
#' @param refreshMinutes Minutes between reads.
#' @param title Title of the page.
#' @param description Text under the title.
#' @param launch.browser Passed to [shiny::runApp()].
#' @param which Which fits to show, as in [DEoptimProgress()]; the cards follow the table.
#'
#' @return `DEoptimDashboard()` runs the app. `DEoptimDashboardApp()` returns it, as a `shiny::shinyApp`.
#' @export
DEoptimDashboard <- function(path = ".", refreshMinutes = 2, title = "DEoptim fits",
                             description = .dashboardDescription,
                             launch.browser = getOption("shiny.launch.browser", interactive()),
                             which = .progressStatuses) {
  shiny::runApp(DEoptimDashboardApp(path, refreshMinutes, title, description, which), launch.browser = launch.browser)
}

## The disclosure arrow of "parameters by generation", drawn here (one in every browser: the native marker
## is hidden, since Bootstrap hides it in some).
.summaryCss <- paste(
  "details > summary { list-style: none; cursor: pointer; color: #5d655f; }",
  "details > summary::-webkit-details-marker { display: none; }",
  "details > summary::before { content: '\\25B8\\00a0'; }",
  "details[open] > summary::before { content: '\\25BE\\00a0'; }")

.appStyle <- function() shiny::tags$style(shiny::HTML(paste(.summaryCss, .cardsCss)))

## The cards fill the window: as many across as fit, none narrower than `.cardMinPx`, at most `.cardsMax`.
.cardMinPx <- 300L
.cardsMax <- 6L
.cardGapPx <- 12L
.cardsCss <- sprintf(
  ".fit-cards { display: grid; gap: %dpx; grid-template-columns: repeat(auto-fill, minmax(max(%dpx, calc((100%% - %d * %dpx) / %d)), 1fr)); } .fit-cards > div { min-width: 0; }",
  .cardGapPx, .cardMinPx, .cardsMax - 1L, .cardGapPx, .cardsMax)

.dashboardDescription <- paste(
  "One card per fit (DEoptim): the objective of the best member, the 10th percentile and the median of the",
  "population at each generation; lower is better. The y axis stops at the 90th percentile, so the first, very",
  "poor generations do not flatten it. Open 'parameters by generation' for each parameter's 10th-90th",
  "percentile band (blue), median (black) and best member (orange). Running: the fit's file changed in the",
  "last 15 min. Finished: the fit is done.")

#' @rdname DEoptimDashboard
#' @export
DEoptimDashboardApp <- function(path = ".", refreshMinutes = 2, title = "DEoptim fits",
                                description = .dashboardDescription, which = .progressStatuses) {
  if (!requireNamespace("shiny", quietly = TRUE)) stop("DEoptimDashboard needs the shiny package")
  if (!requireNamespace("DT", quietly = TRUE)) stop("DEoptimDashboard needs the DT package")
  state <- new.env()
  ui <- shiny::fluidPage(
    ## Bootstrap hides the disclosure triangle of <summary>
    .appStyle(),
    shiny::titlePanel(title),
    shiny::p(description),
    shiny::textOutput("checked"),
    DT::DTOutput("fits"),
    shiny::uiOutput("cards"))
  server <- function(input, output, session) {
    data <- shiny::reactive({
      shiny::invalidateLater(refreshMinutes * 60 * 1000)
      d <- DEoptimProgress(path, state = state, which = which)
      d$fits <- d$fits[order(d$fits$label), ]
      d
    })
    output$checked <- shiny::renderText(paste("read", format(Sys.time(), "%H:%M:%S"), "- refreshes every",
                                              refreshMinutes, "min"))
    ## The table is drawn once; a refresh replaces its data in place, which keeps the sort order, the page
    ## and the search (a re-render would reset them). Numbers and times stay numbers and times, so they
    ## sort as such.
    output$fits <- DT::renderDT(.fitsTable(shiny::isolate(data())$fits))
    proxy <- DT::dataTableProxy("fits")
    shiny::observeEvent(data(), DT::replaceData(proxy, .fitsData(data()$fits), resetPaging = FALSE,
                                                rownames = FALSE), ignoreInit = TRUE)
    ## The cards follow the table: its rows in the order shown (sorted and searched), as positions in
    ## the current data. Until the table reports them, or when they do not fit the data (a refresh changed
    ## the fits), the default order. A click on a header redraws the cards and their plots.
    cardOrder <- shiny::reactive({
      n <- nrow(data()$fits)
      shown <- input$fits_rows_all
      if (is.null(shown) || !all(shown %in% seq_len(n))) seq_len(n) else shown
    })
    ## one card per fit; the parameter plot is drawn when its section is first opened (Shiny does not
    ## draw hidden outputs; "shown" makes it check again)
    output$cards <- shiny::renderUI({
      f <- data()$fits
      shiny::div(class = "fit-cards", lapply(cardOrder(), function(i) shiny::div(
        shiny::tags$h4(paste(f$label[i], "-", tolower(f$status[i]), "- generation", f$generation[i])),
        shiny::plotOutput(paste0("obj_", i), height = "260px"),
        shiny::tags$details(ontoggle = "$(this).trigger('shown')",
                            shiny::tags$summary("parameters by generation"),
                            shiny::plotOutput(paste0("par_", i), height = "520px")))))
    })
    ## output ids are the card's position; the fit is looked up when drawn, so a refresh keeps the plots
    shiny::observe({
      cardOrder()                     # drawn again when the cards are
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

## The columns of the top table, with their types.
.fitsData <- function(f) if (!nrow(f)) data.frame(
  fit = character(), status = character(), generation = integer(), `best objective` = numeric(),
  started = .POSIXct(numeric()), updated = .POSIXct(numeric()), check.names = FALSE) else data.frame(
  fit = f$label, status = f$status, generation = f$generation, `best objective` = round(f$best, 1),
  started = f$started, updated = f$updated, check.names = FALSE)

.fitsTable <- function(f) {
  DT::formatDate(DT::datatable(.fitsData(f), rownames = FALSE, selection = "none",
                               options = list(pageLength = 50, language = list(
                                 emptyTable = "No DEoptim progress files found."))),
                 c("started", "updated"), method = "toLocaleString")
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
  graphics::par(mfrow = c(ceiling(length(nms) / 2), 2), mar = c(2.2, 2.2, 1.4, 0.4), mgp = c(1.2, 0.4, 0), cex.axis = 0.7, cex.main = 0.8, tcl = -0.3)
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
