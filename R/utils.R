#' Converts a list or vector to a scalar buffer
#'
#' @param x An R vector or list
#' @param sc A `spark_connection`
as_scala_buffer <- function(x, sc) {
  array_list <- invoke_new(sc, "java.util.ArrayList")
  for (y in x) invoke(array_list, "add", y)
  sbuf <- sc %>%
    invoke_static("scala.collection.JavaConversions", "asScalaBuffer",
                  array_list)
  return(sbuf)
}

#' Wraps an object in a Scala 'Some' object
#'
#' @param x An R or jobj.
#' @param sc A `spark_connection`
as_scala_some <- function(x, sc) {
  s <- sparklyr::invoke_new(sc, "scala.Some", x)
  return(s)
}

#' Checks whether the input is a scalar
#'
#' @param x input
#' @return TRUE if `x` is a scalar and FALSE otherwise
is.scalar <- function(x) is.atomic(x) && length(x) == 1L

#' Converts an R object to an equivalent Scala object
#'
#' @param x R object coercible to a Scala object
#' @param sc A `spark_connection`
#' @return a jobj reference to a Scala object
as_scala <- function (x, sc, ...) {
  UseMethod("as_scala", x)
}

# R NULL object is represented as a Scala None object.
as_scala.NULL <- function(x, sc, ...) {
  sc %>%
    sparklyr::invoke_static("scala.None$", "MODULE$")
}
