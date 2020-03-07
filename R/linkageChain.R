#' @include utils.R runInference.R
NULL

setOldClass("linkagechain")

new_linkagechain <- function(jobj, projectPath, ...) {
  chain <- list(projectPath = projectPath, jobj = jobj)
  class(chain) <- c("linkagechain", class(chain))
  chain
}

spark_jobj.linkagechain <- function(x, ...) {
  x$jobj
}

#' Load samples of the linkage structure
#'
#' @param sc A `spark_connection`.
#' @param projectPath Path to the project directory.
#' @return A `LinkageChain` jobj.
loadLinkageChain <- function(sc, projectPath) {
  projectPath <- forge::cast_scalar_character(projectPath, id='projectPath')

  jobj <- sc %>%
    sparklyr::invoke_static("com.github.cleanzr.dblink.LinkageChain",
                            "readLinkageChain", projectPath)
  new_linkagechain(jobj, projectPath)
}
