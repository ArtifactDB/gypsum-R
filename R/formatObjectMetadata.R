#' Format object-related metadata
#'
#' Create object-related metadata to validate against the default schema from \code{\link{fetchMetadataSchema}}.
#' This is intended for downstream package developers who are auto-generating metadata documents to be validated by \code{\link{validateMetadata}}.
#'
#' @param x An R object, typically an instance of a Bioconductor class.
#'
#' @return List containing the object-related metadata, typically stored in the \code{applications.takane} field of the metadata. 
#'
#' @author Aaron Lun
#'
#' @examples
#' df <- S4Vectors::DataFrame(alpha=LETTERS, numeric=runif(26))
#' formatObjectMetadata(df)
#' 
#' @export
formatObjectMetadata <- function(x) {
    output <- list()

    if (methods::is(x, "SummarizedExperiment")) {
        # CRAZY SHIT HERE to avoid Suggests'ing these packages.
        senm <- getNamespace("SummarizedExperiment")
        output$summarized_experiment = list(
           rows = nrow(x),
           columns = ncol(x),
           assays = as.list(get("assayNames", envir=senm)(x)),
           column_annotations = as.list(colnames(get("colData", envir=senm)(x)))
        )

        if (methods::is(x, "SingleCellExperiment")) {
            scenm <- getNamespace("SingleCellExperiment")
            output$single_cell_experiment <- list(
                reduced_dimensions = as.list(get("reducedDimNames", envir=scenm)(x)),
                alternative_experiments = as.list(get("altExpNames", envir=scenm)(x))
            )
        }

    } else if (methods::is(x, "DataFrame")) {
        output$data_frame <- list(
            rows = nrow(x),
            column_names = as.list(colnames(x))
        )
    }

    output
}
