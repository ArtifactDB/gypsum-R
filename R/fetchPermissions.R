#' Fetch project permissions 
#'
#' Fetch the permissions for a project.
#' 
#' @param project String containing the project name.
#' @param url String containing the URL of the gypsum REST API.
#' @param config Deprecated and ignored.
#'
#' @return List containing the permissions for this project.
#' This has the following elements:
#' \itemize{
#' \item \code{owners}, a character vector containing the GitHub users or organizations that are owners of this project.
#' \item \code{uploaders}, a list of lists specifying the users or organizations who are authorzied to upload to this project.
#' Each entry is a list with the following fields:
#' \itemize{
#' \item \code{id}, a string containing the GitHub user or organization that is authorized to upload.
#' \item (optional) \code{asset}, a string containing the name of the asset that the uploader is allowed to upload to.
#' If not provided, there is no restriction on the uploaded asset name.
#' \item (optional) \code{version}, a string containing the name of the version that the uploader is allowed to upload to.
#' If not provided, there is no restriction on the uploaded version name.
#' \item (optional) \code{until}, a \link{POSIXct} object containing the expiry date of this authorization.
#' If not provided, the authorization does not expire.
#' \item (optional) \code{trusted}, whether the uploader is trusted.
#' If not provided, defaults to \code{FALSE}.
#' }
#' }
#'
#' @author Aaron Lun
#'
#' @seealso
#' \code{\link{setPermissions}}, to set the permissions.
#'
#' @examples
#' fetchPermissions("test-R")
#'
#' @export
fetchPermissions <- function(project, url=restUrl(), config=NULL) {
    perms <- get_json(paste0(project, "/..permissions"), url=url)

    # Converting everything to POSIX dates.
    for (i in seq_along(perms$uploaders)) {
        current <- perms$uploaders[[i]]
        if ("until" %in% names(current)) {
            perms$uploaders[[i]]$until <- .cast_datetime(current$until)
        }
    }

    perms
}
