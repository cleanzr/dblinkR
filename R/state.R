#' @include utils.R partitioners.R similarityFns.R attribute.R
NULL

setOldClass("dblinkstate")

new_dblinkstate <- function(jobj, ...) {
  dblinkstate <- list(jobj = jobj)
  class(dblinkstate) <- c("dblinkstate", class(dblinkstate))
  dblinkstate
}

spark_jobj.dblinkstate <- function(x, ...) {
  x$jobj
}

#' Initialize the model state
#'
#' @param sc A `spark_connection`
#' @param data A Spark DataFrame or an \R object that can be cast to a
#'   Spark DataFrame
#' @param attributeSpecs A named list of [`Attribute`] objects. Each entry
#'   in the list specifies the model parameters for an entity attribute,
#'   and should match one of the column names (attributes) in
#'   `data`.
#' @param recIdColname Column name in `data` that contains unique record
#'   identifiers.
#' @param partitioner A [`Partitioner-class`] object which specifies how to
#'   partition the space of entities.
#' @param fileIdColname Column name in `data` that contains contains
#'   file/source identifiers for the records. If NULL, the records are assumed
#'   to be from a single file/source.
#' @param populationSize Specifies the prior on the size of the population of
#'   latent entities. An integer specifies a fixed population size, a
#'   a [`ShiftedNegBinomRV`] object specifies a shifted negative binomial
#'   prior.
#' @param randomSeed An integer random seed.
#' @param maxClusterSize A guess at the maximum cluster size in `data`.
#' @return A `state_jobj` object
#'
#' @seealso [`loadState`]
initializeState <- function(sc, data, attributeSpecs, recIdColname,
                            partitioner, populationSize, fileIdColname = NULL,
                            randomSeed = 1L, maxClusterSize = 10L) {
  # Verify input and cast to required data types
  if (!inherits(data, "tbl_spark")) data <- sdf_import(data, sc)
  dataColnames <- sapply(sdf_schema(data), function(x) x$name)

  if (!is.list(attributeSpecs)) stop("attributeSpecs must be a list")
  attributeNames <- names(attributeSpecs)
  if (is.null(attributeNames)) stop("attributeSpecs is missing names")
  missingAttributes <- setdiff(attributeNames, dataColnames)
  if (length(missingAttributes) != 0) {
    stop("data is missing the following attributes: ", paste(missingAttributes))
  }

  recIdColname <- forge::cast_scalar_character(recIdColname, id='recIdColname')
  if (!is.element(recIdColname, dataColnames)) {
    stop("`recIdColname` does not match any columns in `data`")
  }

  if (!is.Partitioner(partitioner))
    stop("`partitioner` must be a Partitioner object")

  if (!is.ShiftedNegBinomRV(populationSize)) {
    populationSize <- forge::cast_scalar_integer(populationSize, id='populationSize')
  }

  if (!is.null(fileIdColname)) {
    fileIdColname <- forge::cast_scalar_character(fileIdColname, id='fileIdColname')
    if (!is.element(fileIdColname, dataColnames)) {
      stop("`fileIdColname` does not match any columns in `data`")
    }
  }

  randomSeed <- forge::cast_scalar_double(randomSeed, id='randomSeed')

  maxClusterSize <- forge::cast_scalar_integer(maxClusterSize, id='maxClusterSize')

  # Initialize Scala/Spark objects
  if (!is.null(fileIdColname)) {
    fileIdColname <- as_scala_some(fileIdColname, sc)
  } else {
    fileIdColname <- scala_none(sc)
  }

  parameters <- sc %>%
    sparklyr::invoke_new("com.github.cleanzr.dblink.Parameters",
                         maxClusterSize)

  attributeSpecs <- lapply(seq_along(attributeSpecs), function(i) {
    Attribute_to_scala(sc, attributeSpecs[[i]], names(attributeSpecs)[i])
  })

  attributeSpecs_seq <- attributeSpecs %>%
    as_scala_buffer(sc) %>%
    sparklyr::invoke("toIndexedSeq")

  partitioner <- Partitioner_as_scala(sc, partitioner, names(attributeSpecs))
  if (is.ShiftedNegBinomRV(populationSize)) {
    populationSize <- shiftedNegBinomRV_to_scala(sc, populationSize)
  } else {
    populationSize <- sc %>%
      sparklyr::invoke_static("com.github.cleanzr.dblink.PopulationSize$",
                              "MODULE$") %>%
      sparklyr::invoke("apply", populationSize)
  }


  sdf_jobj <- spark_dataframe(data)
  state_jobj <- sc %>%
    sparklyr::invoke_static("com.github.cleanzr.dblink.State",
                            "deterministic",
                            sdf_jobj, recIdColname, fileIdColname,
                            attributeSpecs_seq, populationSize, parameters, partitioner,
                            randomSeed)

  new_dblinkstate(state_jobj)
}


#' Load a saved state from disk
#'
#' @param projectPath Path to the project directory.
#' @return A `state_jobj` object.
loadState <- function(sc, projectPath) {
  projectPath <- forge::cast_scalar_character(projectPath, id='path')
  state_jobj <- sc %>%
    sparklyr::invoke_static("com.github.cleanzr.dblink.State", "read",
                            projectPath)
  new_dblinkstate(state_jobj)
}
