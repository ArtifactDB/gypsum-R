#' Fetch project usage details
#'
#' Fetch the quota usage for a project.
#' 
#' @param project String containing the project name.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#'
#' @return Numeric scalar specifying the quota usage for the project, in bytes.
#'
#' @author Aaron Lun
#'
#' @examples
#' fetchUsage("test-R")
#'
#' @seealso
#' \code{\link{refreshUsage}}, to recompute the used quota.
#'
#' @export
fetchUsage <- function(project, url=restUrl(), config=NULL) {
    used <- get_json(paste(project, "..usage", sep="/"), url=url)
    used$total
}
