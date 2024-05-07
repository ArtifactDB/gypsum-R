#' Fetch project quota details
#'
#' Fetch the quota details for a project.
#' 
#' @param project String containing the project name.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#'
#' @return List containing \code{baseline}, the baseline quota at time zero in bytes;
#' \code{growth_rate}, the annual growth rate for the quota in bytes;
#' and \code{year}, the creation year (i.e., time zero) for this project.
#'
#' @author Aaron Lun
#'
#' @examples
#' fetchQuota("test-R")
#'
#' @seealso
#' \code{\link{setQuota}}, to set the quota details.
#'
#' @export
fetchQuota <- function(project, url=restUrl(), config=NULL) {
    get_json(paste(project, "..quota", sep="/"), url=url)
}
