#' Save a file from a version of a project asset
#'
#' Download a file from the gypsum bucket, for a version of an asset of a project.
#'
#' @param project String containing the project name.
#' @param asset String containing the asset name.
#' @param version String containing the version name.
#' @param path String containing the suffix of the object key for the file of interest,
#' i.e., the relative \dQuote{path} inside the version's \dQuote{subdirectory}.
#' The full object key is defined as \code{{project}/{asset}/{version}/{path}}.
#' @param cache String containing the path to the cache directory.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#' @param overwrite Logical scalar indicating whether to overwrite an existing file in \code{cache}.
#' If \code{FALSE} and the file exists in \code{cache}, the download is skipped.
#' 
#' @return The file is downloaded to the local file system. The destination file path is returned.
#'
#' @details
#' The full object key is defined as \code{{project}/{asset}/{version}/{path}}.
#' If no file exists in the project-asset-version combination at \code{path}, 
#' this function will check the \code{..links} file to check whether \code{path} refers to a linked-from file.
#' If so, the contents of the link destination is downloaded to the cache and a link/copy is created at the returned file path.
#'
#' @author Aaron Lun
#' 
#' @seealso
#' \code{\link{saveVersion}}, to save all files with the same prefix.
#'
#' \code{\link{cacheDirectory}}, for file caching.
#' 
#' @examples
#' out <- saveFile("test-R", "basic", "v1", "blah.txt")
#' readLines(out)
#' 
#' @export
saveFile <- function(project, asset, version, path, cache=cacheDirectory(), overwrite=FALSE, url=restUrl(), config=NULL) {
    acquire_lock(cache, project, asset, version)
    on.exit(release_lock(project, asset, version), add=TRUE, after=FALSE)

    object <- paste0(project, "/", asset, "/", version, "/", sanitize_path(path))
    destination <- file.path(cache, BUCKET_CACHE_NAME, project, asset, version, path)
    found <- save_file(object, destination, overwrite=overwrite, url=url, error=FALSE)

    if (!found) {
        link <- resolve_single_link(project, asset, version, path, cache, overwrite=overwrite, url=url)
        if (is.null(link)) {
            stop("'", path, "' does not exist in the bucket")
        }
        if (!file.link(link, destination) && !file.copy(link, destination)) {
            stop("failed to resolve link for '", path, "'")
        }
    }

    destination
}

#' @importFrom jsonlite fromJSON
resolve_single_link <- function(project, asset, version, path, cache, overwrite, url) {
    if (grepl("/", path)) {
        lpath <- paste0(sub("/[^/]+$", "", path), "/..links")
    } else {
        lpath <- "..links"
    }

    lobject <- paste0(project, "/", asset, "/", version, "/", lpath)
    ldestination <- file.path(cache, BUCKET_CACHE_NAME, project, asset, version, lpath)
    if (!save_file(lobject, ldestination, overwrite=overwrite, url=url, error=FALSE)) {
        return(NULL)
    }

    link.info <- fromJSON(ldestination, simplifyVector=FALSE)
    base <- sub(".*/", "", path)
    if (!(base %in% names(link.info))) {
        return(NULL)
    }

    target <- link.info[[base]]
    if ("ancestor" %in% names(target)) {
        target <- target[["ancestor"]]
    }

    tobject <- paste0(target$project, "/", target$asset, "/", target$version, "/", target$path)
    tdestination <- file.path(cache, BUCKET_CACHE_NAME, target$project, target$asset, target$version, target$path)
    save_file(tobject, tdestination, overwrite=overwrite, url=url)
    return(tdestination)
}
