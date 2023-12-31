#' Fetch project permissions 
#'
#' Fetch the permissions for a project.
#' 
#' @param project String containing the project name.
#' @param config Configuration object for the S3 bucket, see \code{\link{publicS3Config}} for details.
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
#' @importFrom jsonlite fromJSON
fetchPermissions <- function(project, config=publicS3Config()) {
    out <- get_file(paste0(project, "/..permissions"), config=config, precheck=TRUE)

    msg <- rawToChar(out)
    if (grepl("^<", msg)) {
        stop(msg)
    }
    perms <- fromJSON(msg, simplifyVector=FALSE)

    # Converting everything to POSIX dates.
    for (i in seq_along(perms$uploaders)) {
        current <- perms$uploaders[[i]]
        if ("until" %in% names(current)) {
            perms$uploaders[[i]]$until <- .cast_datetime(current$until)
        }
    }

    perms
}
