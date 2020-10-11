#' @include state.R
NULL

setOldClass("dblinkresult")

new_dblinkresult <- function(state, projectPath, ..., linkageChain = NULL) {
  result <- list(projectPath = projectPath, state = state,
                 linkageChain = NULL)
  class(result) <- c("dblinkresult", class(result))
}

#' Load a dblink result from disk
#'
#' @param sc A `spark_connection`.
#' @param projectPath path to the project directory.
#' @return A `LinkageChain` jobj.
#' @export
loadResult <- function(sc, projectPath) {
  finalState <- loadState(sc, projectPath)
  linkageChain <- loadLinkageChain(sc, projectPath)
  new_dblinkresult(finalState, projectPath, linkageChain = linkageChain)
}

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
#' @export
runInference <- function(initialState, projectPath, sampleSize,
                         burninInterval = 0L, thinningInterval = 1L,
                         checkpointInterval = 20L, writeBufferSize = 10L,
                         sampler = 'PCG-I') {
  if (!inherits(initialState, "dblinkstate"))
    stop("`initialState` must be a `dblinkstate` object")

  state_jobj <- sparklyr::spark_jobj(initialState)
  sc <- state_jobj$connection

  # Only proceed if we can use checkpoints
  checkpointDir <- sc %>%
    sparklyr::spark_context() %>%
    invoke("getCheckpointDir") %>%
    jobj_class(simple_name = FALSE) %>%
    head(1)
  if (checkpointDir == "scala.None$")
    stop("Spark checkpoint directory is not set. Please use `sparklyr::spark_set_checkpoint_dir` to set a checkpoint directory.")

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

  state_jobj <- sc %>%
    sparklyr::invoke_static("com.github.cleanzr.dblink.Sampler",
                            "sample", state_jobj, sampleSize, projectPath,
                            burninInterval, thinningInterval,
                            checkpointInterval, writeBufferSize,
                            collapsedEntityIds, collapsedEntityValues,
                            sequential)

  finalState <- new_dblinkstate(state_jobj)

  new_dblinkresult(finalState, projectPath, linkageChain = NULL)
}
