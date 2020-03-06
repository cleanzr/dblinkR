setOldClass("DBlinkResult")

#' Run inference using Markov chain Monte Carlo
#'
#' Generates posterior samples by successively applying the Markov transition
#' operator starting from a given initial state. The samples are written to
#' the path provided.
#'
#' @param initialState a `State` jobj which represents the initial state of
#'   the Markov chain
#' @param sampleSize A positive integer specifying the desired number of
#'   samples (after burn-in and thinning)
#' @param projectPath A string specifying the path to save output (includes
#'   samples and diagnostics). HDFS and local filesystems are supported.
#' @param burninInterval A non-negative integer specifying the number of
#'   initial samples to discard as burn-in. The default is 0, which means no
#'   burn-in is applied.
#' @param thinningInterval A positive integer specifying the period for saving
#'   samples to disk. The default value is 1, which means no thinning is
#'   applied.
#' @param checkpointInterval A non-negative integer specifying the period for
#'   checkpointing. This prevents the lineage of the RDD (internal to state)
#'   from becoming too long. Smaller values require more frequent writing to
#'   disk, larger values require more CPU/memory. The default value of 20,
#'   is a reasonable trade-off.
#' @param writeBufferSize A positive integer specifying the number of samples
#'   to queue in memory before writing to disk.
#' @param sampler One of 'PCG-I', 'PCG-II', 'Gibbs' or 'Gibbs-Sequential'.
#' @return a `State` jobj which represents the state at the end of the
#'   Markov chain
#'
#' @seealso [`initializeState`], [`loadState`]
runInference <- function(initialState, projectPath, sampleSize,
                         burninInterval = 0L, thinningInterval = 1L,
                         checkpointInterval = 20L, writeBufferSize = 10L,
                         sampler = 'PCG-I') {
  if (!inherits(initialState, "state_jobj"))
    stop("`initialState` must be a `state_jobj` object")
  # Remove class in this function so that the object can be serialized
  class(initialState) <- Filter(function(x) x != "state_jobj", class(initialState))
  sc <- initialState$connection

  projectPath <- forge::cast_scalar_character(projectPath, id='projectPath')
  sampleSize <- forge::cast_scalar_integer(sampleSize, id='sampleSize')
  burninInterval <- forge::cast_scalar_integer(burninInterval, id='burninInterval')
  thinningInterval <- forge::cast_scalar_integer(thinningInterval, id='thinningInterval')
  checkpointInterval <- forge::cast_scalar_integer(checkpointInterval, id='checkpointInterval')
  writeBufferSize <- forge::cast_scalar_integer(writeBufferSize, id='writeBufferSize')

  collapsedEntityIds <- FALSE
  collapsedEntityValues <- TRUE
  sequential <- FALSE

  if (sampler == 'PCG-I') {
  } else if (sampler == 'PCG-II') {
    collapsedEntityIds <- TRUE
  } else if (sampler == 'Gibbs') {
    collapsedEntityValues <- FALSE
  } else if (sampler == 'Gibbs-Sequential') {
    collapsedEntityValues <- FALSE
    sequential <- TRUE
  } else {
    stop("Unrecognized `sampler`")
  }

  finalState <- sc %>%
    sparklyr::invoke_static("com.github.cleanzr.dblink.Sampler",
                            "sample", initialState, sampleSize, projectPath,
                            burninInterval, thinningInterval,
                            checkpointInterval, writeBufferSize,
                            collapsedEntityIds, collapsedEntityValues,
                            sequential)
  result <- list(projectPath = projectPath, state = finalState,
                 linkageChain = NULL)
  class(result) <- c("DBlinkResult", class(result))
  result
}
