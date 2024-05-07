#' List files for a version
#'
#' List files belonging to a version of a project asset.
#'
#' @param project String containing the project name.
#' @param asset String containing the asset name.
#' @param version String containing the version name.
#' @param prefix String containing the remaining prefix for the object key.
#' If provided, a file is only listed if its object key starts with \code{{project}/{asset}/{version}/{prefix}}.
#' If \code{NULL}, all files associated with this version of the asset are listed.
#' @param include.. Logical scalar indicating whether to list files with \code{/..} in their object keys.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#'
#' @author Aaron Lun
#'
#' @return Character vector of relative paths of files associated with the versioned asset.
#'
#' @examples
#' listFiles("test-R", "basic", "v1")
#' 
#' @export
listFiles <- function(project, asset, version, prefix=NULL, include..=TRUE, url=restUrl(), config=NULL) {
    actual.prefix <- paste(project, asset, version, "", sep="/") # empty string to force trailing slash.
    truncator <- nchar(actual.prefix) + 1L
    if (!is.null(prefix)) {
        actual.prefix <- paste0(actual.prefix, prefix)
    }

    req <- request(paste0(url, "/list?prefix=", uenc(actual.prefix), "&recursive=true"))
    req <- req_error(req, body = function(res) resp_body_json(res)$reason)
    res <- req_perform(req)
    out <- unlist(resp_body_json(res))

    out <- substr(out, truncator, nchar(out))
    if (!include..) {
        out <- out[!startsWith(out, "..") & !grepl("\\/\\.\\.", out)]
    }

    out
}
