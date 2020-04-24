#' @include utils.R linkageChain.R
NULL

setOldClass(c('mostprobableclusters', 'list'))

new_mostprobableclusters <- function(jobj, ...) {
  mpc <- list(jobj = jobj)
  class(mpc) <- c("mostprobableclusters", class(mpc))
  mpc
}

spark_jobj.mostprobableclusters <- function(x, ...) {
  x$jobj
}

#' @rdname mostProbableClusters
#' @param x A `mostprobableclusters` object.
collect.mostprobableclusters <- function(x, ...){
  # Collect as a tibble
  mpc <- x %>%
    sparklyr::spark_jobj() %>%
    sparklyr::sdf_register() %>%
    sparklyr::sdf_collect()
  # Cast cluster to a vector of record ids, rather than a list
  mpc[['cluster']] <- lapply(mpc[['cluster']], simplify2array)
  mpc
}

new_clusters <- function(jobj, ...) {
  clusters <- list(jobj = jobj)
  class(clusters) <- c("clusters", class(clusters))
  clusters
}

spark_jobj.clusters <- function(x, ...) {
  x$jobj
}

# @param x A `clusters` object.
collect.clusters <- function(x){
  # Collect as a tibble
  clusters <- x %>%
    sparklyr::spark_jobj() %>%
    sparklyr::sdf_register() %>%
    sparklyr::sdf_collect()
  # Extract column as list and cast lists to vectors
  clusters <- unlist(clusters, recursive=FALSE, use.names=FALSE) %>%
    lapply(simplify2array)
  class(clusters) <- c('Clusters', class(clusters))
  clusters
}


#' Shared Most Probable Clusters
#'
#' @description
#' Computes a point estimate of the most likely clustering that obeys
#' transitivity constraints based on posterior samples. The method was
#' introduced by Steorts et al. (2016), where it is referred to as the
#' method of _shared most probable maximal matching sets_.
#'
#' @param x A `dblinkresult` object as returned by [`runInference`] or a
#'   `linkagechain` object as returned by [`loadLinkageChain`].
#' @param m_jobj An optional `mostprobableclusters` object as returned by
#'   [`mostProbableClusters`]. If provided, the function can skip
#'   computing the most probable clusters.
#' @return A `clusters` object.
#'
#' @references Steorts, R. C., Hall, R. & Fienberg, S. E. A Bayesian Approach
#' to Graphical Record Linkage and Deduplication. _JASA_ \strong{111},
#' 1660–1672 (2016).
#'
#' @export sharedMostProbableClusters
setGeneric("sharedMostProbableClusters",
           function(x, ...) standardGeneric("sharedMostProbableClusters"))

#' @rdname sharedMostProbableClusters
setMethod("sharedMostProbableClusters", signature = c(x="linkagechain"),
  function(x, ...) {
    jobj <- sparklyr::spark_jobj(x)
    sc <- jobj$connection
    dummy_jobj <- sc %>%
      sparklyr::invoke_new("scala.Predef$DummyImplicit")
    smpc_jobj <- sc %>%
      sparklyr::invoke_static("com.github.cleanzr.dblink.LinkageChain",
                              "sharedMostProbableClusters", jobj, dummy_jobj)
    new_clusters(smpc_jobj)
  }
)

#' @rdname sharedMostProbableClusters
setMethod("sharedMostProbableClusters",
  signature = c(x="mostprobableclusters"),
  function(x, ...) {
    jobj <- sparklyr::spark_jobj(x)
    sc <- jobj$connection
    smpc <- sc %>%
      sparklyr::invoke_static("com.github.cleanzr.dblink.LinkageChain",
                              "sharedMostProbableClusters", jobj)
    new_clusters(smpc_jobj)
  }
)

#' @rdname sharedMostProbableClusters
setMethod("sharedMostProbableClusters", signature = c(x="dblinkresult"),
  function(x, ...) {
    linkageChain <- x$linkageChain
    if (is.null(linkageChain)) {
      sc <- sparklyr::spark_connection_find()
      linkageChain <- loadLinkageChain(sc, x$projectPath)
    }
    sharedMostProbableClusters(linkageChain)
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
#' @param x a `dblinkresult` object as returned by [`runInference`], or
#'   a `linkagestructure` as returned by [`loadLinkageChain`].
#' @return A `mostprobableclusters` object.
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
setMethod("mostProbableClusters", signature = c(x="linkagechain"),
  function(x, ...) {
    jobj <- sparklyr::spark_jobj(x)
    sc <- jobj$connection
    mpc_jobj <- sc %>%
      sparklyr::invoke_static("com.github.cleanzr.dblink.LinkageChain",
                              "mostProbableClusters", jobj)
    new_mostprobableclusters(mpc_jobj)
  }
)

#' @rdname mostProbableClusters
setMethod("mostProbableClusters", signature = c(x="dblinkresult"),
  function(x, ...) {
    linkageChain <- x$linkageChain
    if (is.null(linkageChain)) {
      sc <- sparklyr::spark_connection_find()
      linkageChain <- loadLinkageChain(sc, x$projectPath)
    }
    mostProbableClusters(linkageChain)
  }
)
