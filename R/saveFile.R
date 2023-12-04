#' Save a file from a version of a project asset
#'
#' Download a file for a version of a project from the gypsum bucket.
#'
#' @param project String containing the project name.
#' @param asset String containing the asset name.
#' @param version String containing the version name.
#' @param path String containing the relative path to the file inside the version.
#' @param destination String containing the destination path for the file.
#' If \code{NULL}, a path inside the \code{\link{cacheDirectory}} is used.
#' @param precheck Whether to check if the file exists before attempting a download.
#' This may be omitted if the file is known to exist.
#' @param config Configuration object for the S3 bucket, see \code{\link{publicS3Config}} for details.
#' @param overwrite Logical scalar indicating whether to overwrite an existing file at \code{destination}.
#' If \code{FALSE} and \code{destination} exists, the download is skipped.
#' Defaults to \code{!is.null(destination)}, so no extra downloads are performed if the file is already cached.
#' 
#' @return The file is downloaded to the local file system.
#' The destination file path is invisibly returned.
#'
#' @author Aaron Lun
#' 
#' @seealso
#' \code{\link{savePrefix}}, to save all files with the same prefix.
#'
#' \code{\link{cacheDirectory}}, for file caching.
#' 
#' @examples
#' out <- saveFile("test-R", "upload-check", "v1", "blah.txt")
#' readLines(out)
#' 
#' @export
#' @importFrom aws.s3 object_exists save_object
saveFile <- function(project, asset, version, path, destination=NULL, config=publicS3Config(), precheck=TRUE, overwrite=NULL) {
    if (is.null(destination)) {
        if (is.null(overwrite)) {
            overwrite <- FALSE
        }
        destination <- file.path(cacheDirectory(), "bucket", project, asset, version, path)
        dir.create(dirname(destination), recursive=TRUE, showWarnings=FALSE)
    }

    if (!file.exists(destination) || overwrite) {
        args <- list(
            object=paste0(project, "/", asset, "/", version, "/", path),
            bucket=config$bucket, 
            key=config$key, 
            secret=config$secret, 
            base_url=sub("^http[s]://", "", config$endpoint), 
            region=""
        )

        if (precheck && !do.call(object_exists, args)) {
            stop(
                "no object exists at '", path, 
                "' for version '", version, 
                "' of asset '", asset, 
                "' of project '", project, "'"
            )
        }

        args$file <- destination
        args$parse_response <- FALSE
        do.call(save_object, args)
    }

    invisible(destination)
}