#' @include utils.R betaRV.R similarityFns.R
NULL

setClass("Attribute", slots=c(simFn="SimFn",
                              distortionPrior="BetaRV"))

#' Attribute specification
#'
#' @description
#' Represents an entity attribute that appears in the data: e.g. 'name of
#' company', 'age', 'zip code', etc.
#'
#' @details
#' An `Attribute` object contains all of the model parameters associated
#' with an entity attribute.
#' This includes:
#' * the prior distribution over the distortion probability (likelihood
#'   that an entity attribute is distorted in the data), and
#' * the distance function (parameterizes the distortion distribution).
#'
#' A named list of `Attribute` objects is required when initializing
#' the model (see [`initializeState`]).
#'
#' @param simFn a [`SimFn-class`]` object.
#' @param distortionPrior a [`BetaRV`] object. Specifies the prior
#'   on the distortion probability for the attribute.
#'
#' @return The `Attribute` constructor returns an `Attribute` object.
#' @export
Attribute <- function(simFn, distortionPrior) {
  new("Attribute", simFn=simFn, distortionPrior=distortionPrior)
}

#' @rdname Attribute
#' @param x an `R` object
#' @return `is.Attribute` returns TRUE if the argument is an `Attribute`
#' object and FALSE otherwise.
#' @export
is.Attribute <- function(x) inherits(x, "Attribute")

setClass("CategoricalAttribute", contains = "Attribute")

#' @rdname Attribute
#' @return The `CategoricalAttribute` constructor returns a
#' `CategoricalAttribute` object.
#' It is intended for modeling categorical attributes, and uses a
#' a constant similarity function.
#' @export
CategoricalAttribute <- function(distortionPrior) {
  new("CategoricalAttribute", simFn=ConstantSimFn(),
      distortionPrior=distortionPrior)
}

#' @rdname Attribute
#' @param x an `R` object
#' @return `is.CategoricalAttribute` returns TRUE if the argument is a
#' `CategoricalAttribute` object and FALSE otherwise.
#' @export
is.CategoricalAttribute <- function(x) inherits(x, "CategoricalAttribute")


as_scala.Attribute <- function(x, sc, name) {
  sc %>%
    sparklyr::invoke_new("com.github.cleanzr.dblink.package$Attribute",
                         forge::cast_scalar_character(name),
                         as_scala(x@simFn, sc),
                         as_scala(x@distortionPrior, sc))
}
