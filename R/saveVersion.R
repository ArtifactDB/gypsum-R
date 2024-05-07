#' Save all files for a version of a project asset
#'
#' Download all files associated with a version of an asset of a project from the gypsum bucket.
#'
#' @param project String containing the project name.
#' @param asset String containing the asset name.
#' @param version String containing the version name.
#' @param cache String containing the path to the cache directory.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#' @param overwrite Logical scalar indicating whether to overwrite existing files in the cache.
#' If \code{FALSE} and the files already exist in \code{cache}, the download is skipped.
#' @param concurrent Integer specifying the number of concurrent downloads.
#' @param relink Logical scalar indicating whether links should be resolved, see \code{\link{resolveLinks}}.
#' 
#' @return The version's files are downloaded to the local file system, and the path to the local subdirectory is returned.
#'
#' @author Aaron Lun
#' 
#' @seealso
#' \code{\link{saveFile}}, to save a single file.
#'
#' \code{\link{cacheDirectory}}, for file caching.
#' 
#' @examples
#' out <- saveVersion("test-R", "basic", "v1")
#' list.files(out, recursive=TRUE, all.files=TRUE)
#' 
#' @export
saveVersion <- function(project, asset, version, cache=cacheDirectory(), overwrite=FALSE, relink=TRUE, concurrent=1, url=restUrl(), config=NULL) {
    acquire_lock(cache, project, asset, version)
    on.exit(release_lock(project, asset, version), add=TRUE, after=FALSE)
    destination <- file.path(cache, BUCKET_CACHE_NAME, project, asset, version)

    # If this version's directory was previously cached in its complete form, we skip it.
    completed <- file.path(cache, "status", project, asset, version, "COMPLETE")

    if (!file.exists(completed) || overwrite) {
        listing <- listFiles(project, asset, version, url=url)
        FUN <- function(x) {
            save_file(
                path=paste(project, asset, version, x, sep="/"), 
                destination=file.path(destination, x), 
                overwrite=overwrite, 
                url=url
            )
        }

        if (concurrent == 1L) {
            lapply(listing, FUN)
        } else {
            cl <- makeCluster(concurrent)
            on.exit(stopCluster(cl), add=TRUE, after=FALSE)
            parLapply(cl, listing, FUN)
        }

        if (relink) {
            resolveLinks(project, asset, version, cache=cache, overwrite=overwrite, url=url)
        }

        # Marking it as complete.
        dir.create(dirname(completed), recursive=TRUE, showWarnings=FALSE)
        write(character(0), file=completed)
    }

    destination
}
