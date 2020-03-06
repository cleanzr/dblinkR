#' @include utils.R
NULL

.check_BetaRV <- function(object) {
  errors = character()
  if (!is.scalar(object@shape1)) {
    errors <- c(errors, "shape1 must be a scalar")
  }
  if (!is.scalar(object@shape2)) {
    errors <- c(errors, "shape2 must be a scalar")
  }
  if (object@shape1 <= 0) {
    errors <- c(errors, "shape1 must be positive")
  }
  if (object@shape2 <= 0) {
    errors <- c(errors, "shape2 must be positive")
  }
  if (!is.finite(object@shape1)) {
    errors <- c(errors, "shape1 must be finite")
  }
  if (!is.finite(object@shape2)) {
    errors <- c(errors, "shape2 must be finite")
  }
  if (length(errors)==0) TRUE else errors
}

setClass("BetaRV", slots = c(shape1 = "numeric", shape2 = "numeric"),
         validity=.check_BetaRV)

#' Beta-distributed Random Variable
#'
#' @description
#' Represents a random variable with a Beta distribution.
#'
#' @details
#' The density of the Beta distribution with parameters \eqn{shape1 = a} and
#' \eqn{shape2 = b} is given by
#' \deqn{p(X = x) = \frac{\Gamma(a+b)}{\Gamma(a) \Gamma(b)} x^{a-1} (1-x)^{b-1}.}
#' for \eqn{0 < x < 1}, \eqn{a > 0} and \eqn{b > 0}.
#'
#' @param shape1,shape2 positive shape parameters of the Beta distribution
#' @return The `BetaRV` constructor returns a `BetaRV` object,
#' which is a subclass of [`RV-class`].
#'
#' @examples
#' # A uniform distribution on the unit interval
#' x <- BetaRV(1, 1)
#' mean(x)
#'
#' @seealso Other random variables defined in this package
#' include [`GammaRV`], [`DirichletRV`] and [`ShiftedNegBinomRV`].
BetaRV <- function(shape1, shape2) {
  new("BetaRV", shape1=shape1, shape2=shape2)
}

betaRV_to_scala <- function(sc, beta) {
  sc %>%
    sparklyr::invoke_new("com.github.cleanzr.dblink.package$BetaShapeParameters",
                         beta@shape1,
                         beta@shape2)
}
