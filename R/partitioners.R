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
#' Partitions the space of entities into blocks using a k-d tree. Each node 
#' of the tree is associated with a splitting rule that divides the input 
#' space into two parts, based on the value of one of the attributes. 
#' The tree is fitted using the observed records, and the splits are 
#' chosen to yield a balanced tree. The depth of the tree and the attributes 
#' used for splitting at each level are user-specified parameters.
#' 
#' @param numLevels The depth/number of levels of the tree. The partitions are
#'   the leaves of the tree, hence the number of partitions is given by
#'   `2^numLevels`.
#' @param attributes The attributes used for splitting at each level of the 
#'   tree are taken by cycling through this vector. For example, if 
#'   `attributes = c("A", "B")`, the split at the 0-th level is based on 
#'   attribute "A", the split at the 1-st level is based on attribute "B", 
#'   the split at the 2-nd level is based on attribute "A", etc.
KDTreePartitioner <- function(numLevels, attributes) {
  new("KDTreePartitioner", numLevels=as.integer(numLevels), attributes=attributes)
}

as_scala.Partitioner <- function(partitioner, sc, attributeNames) {
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
