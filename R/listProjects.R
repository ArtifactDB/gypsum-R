#' List all projects
#'
#' List all projects in the gypsum backent.
#'
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#'
#' @author Aaron Lun
#'
#' @return Character vector of project names.
#'
#' @examples
#' if (interactive()) {
#'     listProjects()
#' }
#' 
#' @export
listProjects <- function(url=restUrl(), config=NULL) {
    list_for_prefix(NULL, url=url)
}
