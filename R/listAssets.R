#' List assets
#'
#' List all assets in a project.
#'
#' @param project String containing the project name.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#'
#' @author Aaron Lun
#'
#' @return Character vector of asset names.
#'
#' @examples
#' listAssets("test-R")
#' 
#' @export
listAssets <- function(project, url=restUrl(), config=NULL) {
    list_for_prefix(paste0(project, "/"), url=url)
}
