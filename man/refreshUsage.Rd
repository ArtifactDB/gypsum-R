% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/refreshUsage.R
\name{refreshUsage}
\alias{refreshUsage}
\title{Refresh the quota usage}
\usage{
refreshUsage(project, url = restUrl(), token = accessToken())
}
\arguments{
\item{project}{String containing the project name.}

\item{url}{String containing the URL of the gypsum REST API.}

\item{token}{String containing a GitHub access token to authenticate to the gypsum REST API.
The token must refer to a gypsum administrator account.}
}
\value{
Numeric scalar specifying the total quota usage of this project, in bytes.
}
\description{
Recompute the quota usage of a project.
This is useful on rare occasions where multiple simultaneous uploads cause the usage calculations to be out of sync.
}
\examples{
if (interactive()) {
    refreshUsage("test-R")
}

}
\seealso{
\code{\link{fetchUsage}}, to get the usage without recomputing it.
}
\author{
Aaron Lun
}
