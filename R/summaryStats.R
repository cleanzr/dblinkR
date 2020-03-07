#' @include linkageChain.R
NULL

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

#' Cluster size distribution along the linkage chain
#'
#' @description TODO
#'
#' @param x A `linkagechain` or `dblinkresult` object.
#' @return A tibble representing the cluster size distribution along the chain.
#'
#' @seealso [`partitionSizes`], [`loadDiagnostics`]
#' @export clusterSizeDistribution
setGeneric("clusterSizeDistribution",
           function(x, ...) standardGeneric("clusterSizeDistribution"))

#' @rdname clusterSizeDistribution
setMethod("clusterSizeDistribution", signature = c(x="linkagechain"),
  function(x, ...) {
    jobj <- sparklyr::spark_jobj(x)
    sc <- jobj$connection
    clustSizeDist <- sc %>%
      sparklyr::invoke_static("com.github.cleanzr.dblink.LinkageChain",
                              "clusterSizeDistribution", jobj) %>%
      sparklyr::sdf_register() %>%
      sparklyr.nested::sdf_explode(`_2`, is_map = TRUE) %>%
      sparklyr::sdf_collect()
    colnames(clustSizeDist) <- c("iteration", "clusterSize", "frequency")
    clustSizeDist
  }
)

#' @rdname clusterSizeDistribution
setMethod("clusterSizeDistribution", signature = c(x="dblinkresult"),
  function(x, ...) {
    linkageChain <- x$linkageChain
    if (is.null(linkageChain)) {
      sc <- sparklyr::spark_connection_find()
      linkageChain <- loadLinkageChain(sc, x$projectPath)
    }
    clusterSizeDistribution(linkageChain)
  }
)



#' Partition sizes along the linkage chain
#'
#' @description TODO
#'
#' @param x A `linkagechain` or `dblinkresult` object.
#' @return A tibble representing the partition sizes along the chain.
#'
#' @seealso [`clusterSizeDistribution`], [`loadDiagnostics`]
#' @export partitionSizes
setGeneric("partitionSizes",
           function(x, ...) standardGeneric("partitionSizes"))

#' @rdname partitionSizes
setMethod("partitionSizes", signature = c(x="linkagechain"),
  function(x) {
    jobj <- sparklyr::spark_jobj(x)
    sc <- jobj$connection
    partSizes <- sc %>%
      sparklyr::invoke_static("com.github.cleanzr.dblink.LinkageChain",
                              "partitionSizes", jobj) %>%
      sparklyr::sdf_register() %>%
      sparklyr.nested::sdf_explode(`_2`, is_map = TRUE) %>%
      sparklyr::sdf_collect()
    colnames(partSizes) <- c("iteration", "partitionId", "size")
    partSizes
  }
)

#' @rdname partitionSizes
setMethod("partitionSizes", signature = c(x="dblinkresult"),
  function(x, ...) {
    linkageChain <- x$linkageChain
    if (is.null(linkageChain)) {
      sc <- sparklyr::spark_connection_find()
      linkageChain <- loadLinkageChain(sc, x$projectPath)
    }
    partitionSizes(linkageChain)
  }
)
