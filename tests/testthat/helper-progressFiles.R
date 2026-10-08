## A synthetic DEoptim progress file: 2 generations of an objective and one parameter `a`. `age`: seconds
## since its last change.
writeProgress <- function(file, finished = FALSE, age = 0) {
  dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
  rows <- data.frame(time = "2026-10-08T10:00:00", generation = rep(1:2, each = 2),
                     variable = rep(c("objective", "a"), 2), best = c(5, 0.5, 4, 0.4),
                     q10 = c(6, 0.2, 5, 0.3), median = c(8, 0.5, 7, 0.5), q90 = c(9, 0.8, 8, 0.7))
  if (finished) rows <- rbind(rows, data.frame(time = "2026-10-08T10:05:00", generation = 2, variable = "FINISHED",
                                               best = NA, q10 = NA, median = NA, q90 = NA))
  data.table::fwrite(rows, file)
  Sys.setFileTime(file, Sys.time() - age)
  file
}
