spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(
    # TODO: Use Spark Packages instead
    jars = c(
      system.file(
        sprintf("java/dblink-%s-%s.jar", spark_version, scala_version),
        package = "dblinkR"
      )
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
