#' Partitioner
#'
#' @description
#' Virtual class for a partitioner
setClass("Partitioner")

is.Partitioner <- function(x) inherits(x, "Partitioner")

.check_KDTreePartitioner <- function(object) {
  errors = character()
  if (!is.scalar(object@numLevels) | object@numLevels < 0) {
    errors <- c(errors, "numLevels must be a non-negative scalar integer")
  }
  if (length(errors)==0) TRUE else errors
}

setClass("KDTreePartitioner", slots = c(numLevels = "integer",
                                        attributes = "character"),
         validity=.check_KDTreePartitioner, contains = "Partitioner")

#' k-d tree partitioner
#'
#' @param numLevels the depth/number of levels of the tree. The partitions are
#'   the leaves of the tree, hence the number of partitions is given by
#'   `2^numLevels`.
#' @param attributes splits are performed by cycling through the attributes
#'   in this vector.
KDTreePartitioner <- function(numLevels, attributes) {
  new("KDTreePartitioner", numLevels=as.integer(numLevels), attributes=attributes)
}

Partitioner_as_scala <- function(sc, partitioner, attributeNames) {
  if (!inherits(partitioner, "KDTreePartitioner"))
    stop("partitioner of type", class(partitioner), "not supported")

  orderingInt <- sc %>% sparklyr::invoke_new("scala.math.Ordering$Int$")

  attributeIds <- seq_along(attributeNames) - 1
  names(attributeIds) <- names(attributeNames)
  attributeIds <- attributeIds[partitioner@attributes]

  attributeIds <- attributeIds %>%
    as_scala_buffer(sc) %>%
    invoke("toIndexedSeq")

  sc %>%
    sparklyr::invoke_new("com.github.cleanzr.dblink.partitioning.KDTreePartitioner",
                         partitioner@numLevels,
                         attributeIds,
                         orderingInt)
}
