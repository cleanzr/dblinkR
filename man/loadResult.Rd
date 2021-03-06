% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/runInference.R
\name{loadResult}
\alias{loadResult}
\title{Load a dblink result from disk}
\usage{
loadResult(sc, projectPath)
}
\arguments{
\item{sc}{A \code{spark_connection}.}

\item{projectPath}{path to the project directory.}
}
\value{
A \code{LinkageChain} jobj.
}
\description{
Load a dblink result from disk
}
