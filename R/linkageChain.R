#' @include runInference.R
NULL

setOldClass("spark_jobj")
setOldClass("mostprobableclusters_jobj")
setOldClass("clusters_jobj")
setOldClass("linkagechain_jobj")
setOldClass("partitionsizes_jobj")
setOldClass("clustersizedist_jobj")

#' Load samples of the linkage structure
#'
#' @param sc A `spark_connection`.
#' @param projectPath Path to the project directory.
#' @return A `LinkageChain` jobj.
loadLinkageChain <- function(sc, projectPath) {
  projectPath <- forge::cast_scalar_character(projectPath, id='projectPath')

  rdd_jobj <- sc %>%
    sparklyr::invoke_static("com.github.cleanzr.dblink.LinkageChain",
                            "read", projectPath)

  lchain_jobj <- sc %>%
    sparklyr::invoke_new("com.github.cleanzr.dblink.LinkageChain", rdd_jobj)
  class(lchain_jobj) <- c("linkagechain_jobj", class(lchain_jobj))
  lchain_jobj
}

#' Load a dblink result from disk
#'
#' @param sc A `spark_connection`.
#' @param projectPath path to the project directory.
#' @return A `LinkageChain` jobj.
loadDBlinkResult <- function(sc, projectPath) {
  finalState <- loadState(sc, projectPath)
  linkageChain <- loadLinkageChain(sc, projectPath)
  result <- list(projectPath = projectPath, state = finalState,
                 linkageChain = linkageChain)
  class(result) <- c("DBlinkResult", class(result))
  result
}

#' Shared Most Probable Clusters
#'
#' @description
#' Computes a point estimate of the most likely clustering that obeys
#' transitivity constraints based on posterior samples. The method was
#' introduced by Steorts et al. (2016), where it is referred to as the
#' method of _shared most probable maximal matching sets_.
#'
#' @param x An [`DBlinkResult`] object as returned by [`runInference`] or a
#'   [`linkagechain_jobj`] object as returned by [`loadLinkageChain`].
#' @param m_jobj An optional `mostprobableclusters_jobj` object as returned by
#'   [`mostProbableClusters`]. If provided, the function can skip
#'   computing the most probable clusters.
#' @return a `clusters_jobj` object.
#'
#' @references Steorts, R. C., Hall, R. & Fienberg, S. E. A Bayesian Approach
#' to Graphical Record Linkage and Deduplication. _JASA_ \strong{111},
#' 1660–1672 (2016).
#'
#' @export sharedMostProbableClusters
setGeneric("sharedMostProbableClusters",
           function(x, m_jobj=NULL, ...) standardGeneric("sharedMostProbableClusters"))

#' @rdname sharedMostProbableClusters
setMethod("sharedMostProbableClusters",
          signature = c(x="linkagechain_jobj", m_jobj="NULL"),
  function(x, m_jobj, ...) {
    smpc <- sparklyr::invoke(x, "sharedMostProbableClusters")
    class(smpc) <- c("clusters_jobj", class(smpc))
    smpc
  }
)

#' @rdname sharedMostProbableClusters
setMethod("sharedMostProbableClusters",
          signature = c(x="linkagechain_jobj", m_jobj="mostprobableclusters_jobj"),
  function(x, m_jobj, ...) {
    class(m_jobj) <- Filter(function(x) x != "mostprobableclusters_jobj", class(m_jobj))
    smpc <- sparklyr::invoke(x, "sharedMostProbableClusters", m_jobj)
    class(smpc) <- c("clusters_jobj", class(smpc))
    smpc
  }
)

#' @rdname sharedMostProbableClusters
setMethod("sharedMostProbableClusters", signature = c(x="DBlinkResult", m_jobj="ANY"),
  function(x, m_jobj, ...) {
    linkageChain <- x$linkageChain
    if (is.null(linkageChain)) {
      sc <- sparklyr::spark_connection_find()
      linkageChain <- loadLinkageChain(sc, x$projectPath)
    }
    sharedMostProbableClusters(linkageChain, m_jobj)
  }
)


#' Most Probable Clusters
#'
#' @description
#' Computes the most probable cluster for each record in the data set based on
#' posterior samples. The collection of most probable clusters is not
#' guaranteed to obey transitivity of closure. To obtain a transitive
#' clustering, one can apply the \code{\link{sharedMostProbableClusters}}
#' function to the output of this function. In (Steorts et al. 2016), the
#' most probable clusters are referred to as \emph{most probable maximal
#' matching sets}.
#'
#' @param x a [`DBlinkResult-class`] object as returned by [`runInference`], or
#'   a [`linkagestructure_jobj`] as returned by [`readLinkageStructure`].
#' @return A `mostprobableclusters_jobj` object.
#'
#' @references Steorts, R. C., Hall, R. & Fienberg, S. E. A Bayesian Approach
#' to Graphical Record Linkage and Deduplication. \emph{JASA} \strong{111},
#' 1660–1672 (2016).
#'
#' @seealso
#' The [`sharedMostProbableClusters`] function computes a point estimate
#' from the most probable clusters (the output of this function), which
#' obeys transitivity constraints.
#' @export mostProbableClusters
setGeneric("mostProbableClusters",
           function(x, ...) standardGeneric("mostProbableClusters"))

#' @rdname mostProbableClusters
setMethod("mostProbableClusters", signature = c(x="linkagechain_jobj"),
  function(x, ...) {
    mpc <- sparklyr::invoke(x, "mostProbableClusters")
    class(mpc) <- c("mostprobableclusters_jobj", class(mpc))
    mpc
  }
)

#' @rdname mostProbableClusters
setMethod("mostProbableClusters", signature = c(x="DBlinkResult"),
  function(x, ...) {
    linkageChain <- x$linkageChain
    if (is.null(linkageChain)) {
      sc <- sparklyr::spark_connection_find()
      linkageChain <- loadLinkageChain(sc, x$projectPath)
    }
    mostProbableClusters(linkageChain)
  }
)

setGeneric("clusterSizeDistribution",
           function(x, ...) standardGeneric("clusterSizeDistribution"))

setMethod("clusterSizeDistribution", signature = c(x="linkagechain_jobj"),
  function(x, ...) {
    clustSizeDist <- sparklyr::invoke(x, "clusterSizeDistribution")
    class(clustSizeDist) <- c("clustersizedist_jobj", class(clustSizeDist))
    clustSizeDist
  }
)

setMethod("clusterSizeDistribution", signature = c(x="DBlinkResult"),
  function(x, ...) {
    linkageChain <- x$linkageChain
    if (is.null(linkageChain)) {
      sc <- sparklyr::spark_connection_find()
      linkageChain <- loadLinkageChain(sc, x$projectPath)
    }
    clusterSizeDistribution(linkageChain)
  }
)

setGeneric("partitionSizes",
           function(x, ...) standardGeneric("partitionSizes"))

setMethod("partitionSizes", signature = c(x="linkagechain_jobj"),
  function(x) {
    psizes <- sparklyr::invoke(x, "partitionSizes")
    class(psizes) <- c("partitionsizes_jobj", class(psizes))
    psizes
  }
)

setMethod("partitionSizes", signature = c(x="DBlinkResult"),
  function(x, ...) {
    linkageChain <- x$linkageChain
    if (is.null(linkageChain)) {
      sc <- sparklyr::spark_connection_find()
      linkageChain <- loadLinkageChain(sc, x$projectPath)
    }
    partitionSizes(linkageChain)
  }
)

setGeneric("collect", function(x, ...) standardGeneric("collect"))

setMethod("collect", signature = c(x = "clusters_jobj"),
  function(x){
    # Collect as a tibble
    clusters <- x %>%
      sparklyr::sdf_register() %>%
      sparklyr::sdf_collect()
    # Extract column as list and cast lists to vectors
    clusters <- unlist(clusters, recursive=FALSE, use.names=FALSE) %>%
      lapply(simplify2array)
    class(clusters) <- c('Clusters', class(clusters))
    clusters
  }
)

#' @rdname mostProbableClusters
#' @param x A `mostprobableclusters_jobj` object.
setMethod("collect", signature = c(x = "mostprobableclusters_jobj"),
  function(x, ...){
    # Collect as a tibble
    mpc <- x %>% sparklyr::sdf_register() %>% sparklyr::sdf_collect()
    # Cast cluster to a vector of record ids, rather than a list
    mpc[['cluster']] <- lapply(mpc[['cluster']], simplify2array)
    mpc
  }
)

setMethod("collect", signature = c(x = "clustersizedist_jobj"),
  function(x, ...){
    # Collect as a tibble
    clustSizeDist <- x %>%
      sparklyr::sdf_register() %>%
      sparklyr.nested::sdf_explode(`_2`, is_map = TRUE) %>%
      sparklyr::sdf_collect()
    colnames(clustSizeDist) <- c("iteration", "clusterSize", "frequency")
    clustSizeDist
  }
)

setMethod("collect", signature = c(x = "partitionsizes_jobj"),
  function(x, ...){
    # Collect as a tibble
    partSizes <- x %>%
      sparklyr::sdf_register() %>%
      sparklyr.nested::sdf_explode(`_2`, is_map = TRUE) %>%
      sparklyr::sdf_collect()
    colnames(partSizes) <- c("iteration", "partitionId", "size")
    partSizes
  }
)
