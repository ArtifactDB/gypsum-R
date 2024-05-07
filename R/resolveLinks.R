#' Resolve links in the cache directory
#'
#' Create hard links (or copies, if filesystem links are not supported) for linked-from files to their link destinations.
#'
#' @param project String containing the project name.
#' @param asset String containing the asset name.
#' @param version String containing the version name.
#' @param cache String containing the path to the cache directory.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#' @param overwrite Logical scalar indicating whether to replace existing files at the linked-from paths.
#' 
#' @return \code{NULL} is returned on successful completion.
#'
#' @author Aaron Lun
#'
#' @examples
#' cache <- tempfile()
#' saveVersion("test-R", "basic", "v3", relink=FALSE, cache=cache)
#' list.files(cache, recursive=TRUE, all.files=TRUE)
#' 
#' resolveLinks("test-R", "basic", "v3", cache=cache)
#' list.files(cache, recursive=TRUE, all.files=TRUE)
#'
#' @export
resolveLinks <- function(project, asset, version, cache=cacheDirectory(), overwrite=FALSE, url=restUrl(), config=NULL) {
    acquire_lock(cache, project, asset, version)
    on.exit(release_lock(project, asset, version), add=TRUE, after=FALSE)
    destination <- file.path(cache, BUCKET_CACHE_NAME, project, asset, version)

    manifests <- list()
    selfmanifest <- fetchManifest(project, asset, version, cache=cache, url=url)
    manifests[[paste(project, asset, version, sep="/")]] <- selfmanifest

    for (l in names(selfmanifest)) {
        entry <- selfmanifest[[l]]
        if (is.null(entry$link)) {
            next
        } 

        oldloc <- file.path(project, asset, version, l) 
        if (file.exists(oldloc) && !overwrite) {
            next
        }
        linkdata <- entry$link

        if (!is.null(linkdata$ancestor)) {
            linkdata <- linkdata$ancestor
        }
        out <- saveFile(linkdata$project, linkdata$asset, linkdata$version, linkdata$path, cache=cache, url=url, overwrite=overwrite)

        oldpath <- file.path(cache, BUCKET_CACHE_NAME, oldloc)
        unlink(oldpath, force=TRUE) # remove any existing file.
        dir.create(dirname(oldpath), showWarnings=FALSE, recursive=TRUE)
        if (!file.link(out, oldpath) && !file.copy(out, oldpath)) {
            stop("failed to resolve link for '", l, "'")
        }
    }

    invisible(NULL)
}
