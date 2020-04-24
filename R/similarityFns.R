

.check_SimFn <- function(object) {
  errors = character()
  if (!is.scalar(object@threshold) | object@threshold < 0) {
    errors <- c(errors, "`threshold` must a non-negative scalar")
  }
  if (!is.scalar(object@maxSimilarity) | object@maxSimilarity < object@threshold) {
    errors <- c(errors, "`maxSimilarity` must be a scalar >= `threshold`")
  }
  if (length(errors)==0) TRUE else errors
}

#' Similarity function
#'
#' @description
#' Virtual class for a similarity function
setClass("SimFn", slots = c(threshold = "numeric", maxSimilarity = "numeric"),
         contains = "VIRTUAL", validity = .check_SimFn)

.check_ConstantSimFn <- function(object) {
  errors = character()
  if (object@threshold != 0) {
    errors <- c(errors, "threshold must be zero for a ConstantSimFn")
  }
  if (object@maxSimilarity != 0) {
    errors <- c(errors, "maxSimilarity must be zero for a ConstantSimFn")
  }
  if (length(errors)==0) TRUE else errors
}

setClass("ConstantSimFn", contains = "SimFn", validity = .check_ConstantSimFn)

#' Constant similarity function
#'
#' Represents a similarity function that returns zero for all pairs of values
#'
#' @return a `ConstantSimFn` object
ConstantSimFn <- function() {
  new("ConstantSimFn", threshold = 0, maxSimilarity = 0)
}

#' Normalized Levenshtein similarity function
setClass("LevenshteinSimFn", contains = "SimFn")

#' Levenshtein similarity function
#'
#' @param threshold Similarities below this threshold will be set to zero. A
#'   higher threshold improves the efficiency of the inference, possibly at
#'   the expense of accuracy.
#' @param maxSimilarity Similarities will be in the range `[0, maxSimilarity]`.
#' @return a `LevenshteinSimFn` object
LevenshteinSimFn <- function(threshold, maxSimilarity) {
  new("LevenshteinSimFn", threshold = threshold, maxSimilarity = maxSimilarity)
}

# @param x A `SimFn` object
# @param sc A `spark_connection`
as_scala.SimFn <- function(x, sc) {
  switch(class(x)[1],
         ConstantSimFn = sparklyr::invoke_static(sc,
                                                 "com.github.cleanzr.dblink.SimilarityFn$ConstantSimilarityFn$", "MODULE$"),
         LevenshteinSimFn = sparklyr::invoke_new(sc,
                                                 "com.github.cleanzr.dblink.SimilarityFn$LevenshteinSimilarityFn",
                                                 x@threshold,
                                                 x@maxSimilarity))
}
