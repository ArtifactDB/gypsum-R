#' Fetch version manifest 
#'
#' Fetch the manifest for a version of an asset of a project.
#'
#' @inheritParams saveFile
#' @param cache String containing the cache directory.
#' If \code{NULL}, no caching is performed.
#' @param overwrite Logical scalar indicating whether to overwrite an existing file in \code{cache}, if one is present.
#'
#' @author Aaron Lun
#' 
#' @return List containing the manifest for this version.
#' Each element is named after the relative path of a file in this version.
#' The value of each element is another list with the following fields:
#' \itemize{
#' \item \code{size}, an integer specifying the size of the file in bytes.
#' \item \code{md5sum}, a string containing the hex-encoded MD5 checksum of the file.
#' \item \code{link} (optional): a list specifying the link destination for a file. 
#' This contains the strings \code{project}, \code{asset}, \code{version} and \code{path}.
#' If the link destination is itself a link, an \code{ancestor} list will be present that specifies the final location of the file after resolving all intermediate links.
#' }
#' 
#' @examples
#' fetchManifest("test-R", "basic", "v1")
#' 
#' @export
fetchManifest <- function(project, asset, version, cache=cacheDirectory(), overwrite=FALSE, url=restUrl(), config=NULL) {
    get_cacheable_json(
       project, 
       asset, 
       version, 
       "..manifest", 
       url=url,
       cache=cache,
       overwrite=overwrite
    )
}
