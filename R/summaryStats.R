#' Load diagnostics from disk
#'
#' @param sc A `spark_connection`
#' @param projectPath Path to the project directory.
#' @return a table containing diagnostics along the Markov chain
loadDiagnostics <- function(sc, projectPath) {
  # TODO Use the spark context to read
  diagPath <- paste(projectPath, "diagnostics.csv", sep = "/")
  diagnostics <- sparklyr::spark_read_csv(sc, path = diagPath)
  sparklyr::collect(diagnostics)
}
