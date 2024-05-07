#' Fetch version summary
#'
#' Fetch the summary for a version of an asset of a project.
#'
#' @inheritParams fetchManifest
#'
#' @author Aaron Lun
#' 
#' @return List containing the summary for this version, with the following fields:
#' \itemize{
#' \item \code{upload_user_id}, string containing the identity of the uploader.
#' \item \code{upload_start}, a \link{POSIXct} object containing the upload start time.
#' \item \code{upload_finish}, a \link{POSIXct} object containing the upload finish time.
#' \item \code{on_probation} (optional), a logical scalar indicating whether the upload is probational.
#' If missing, this can be assumed to be \code{FALSE}.
#' }
#' 
#' @examples
#' fetchSummary("test-R", "basic", "v1")
#' 
#' @export
fetchSummary <- function(project, asset, version, cache=cacheDirectory(), overwrite=FALSE, url=restUrl(), config=NULL) {
    out <- get_cacheable_json(
        project, 
        asset, 
        version, 
        "..summary", 
        cache=cache, 
        overwrite=overwrite,
        url=url
    )

    out$upload_start <- .cast_datetime(out$upload_start)
    out$upload_finish <- .cast_datetime(out$upload_finish)

    if (isTRUE(out$on_probation) && !is.null(cache)) {
        # Remove cached entry because it might not be accurate in subsequent calls.
        unlink(file.path(cache, BUCKET_CACHE_NAME, project, asset, version, "..summary"))
    }

    out
}
