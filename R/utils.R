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

#' Converts an object to a 'Some' object
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

scala_none <- function(sc) {
  sc %>%
    sparklyr::invoke_static("scala.None$", "MODULE$")
}
