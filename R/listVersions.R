#' List asset versions
#'
#' List all versions of a project asset.
#'
#' @param project String containing the project name.
#' @param asset String containing the asset name.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#'
#' @author Aaron Lun
#'
#' @return Character vector of versions.
#'
#' @examples
#' listVersions("test-R", "basic")
#' 
#' @export
listVersions <- function(project, asset, url=restUrl(), config=NULL) {
    list_for_prefix(paste0(project, "/", asset, "/"), url=url)
}
