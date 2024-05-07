#' Fetch the latest version
#'
#' Fetch the latest version of a project's asset.
#' 
#' @param project String containing the project name.
#' @param asset String containing the asset name.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#'
#' @return String containing the latest version of the project.
#'
#' @author Aaron Lun
#'
#' @examples
#' fetchLatest("test-R", "basic")
#'
#' @seealso
#' \code{\link{refreshLatest}}, to refresh the latest version.
#'
#' @export
fetchLatest <- function(project, asset, url=restUrl(), config=NULL) { 
    vers <- get_json(paste(project, asset, "..latest", sep="/"), url=url)
    vers$version
}
