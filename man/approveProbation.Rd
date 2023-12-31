% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/approveProbation.R
\name{approveProbation}
\alias{approveProbation}
\title{Approve a probational upload}
\usage{
approveProbation(
  project,
  asset,
  version,
  url = restUrl(),
  token = accessToken()
)
}
\arguments{
\item{project}{String containing the project name.}

\item{asset}{String containing the asset name.}

\item{version}{String containing the version name.}

\item{url}{String containing the URL of the gypsum REST API.}

\item{token}{String containing a GitHub access token to authenticate to the gypsum REST API.
The token must refer to an owner of \code{project}.}
}
\value{
\code{NULL} is invisibly returned upon successful approval.
}
\description{
Pretty much as it says: approve a probational upload of a version of a project's asset.
This removes the \code{on_probation} tag from the uploaded version.
}
\examples{
if (interactive()) {
    # Mocking up a versioned asset.
    init <- startUpload(
        project="test-R", 
        asset="probation-approve", 
        version="v1", 
        files=character(0),
        probation=TRUE
    )
    completeUpload(init) 

    # Approving the probation:
    approveProbation("test-R", "probation-approve", "v1")

    # Just cleaning up after we're done.
    removeProjectAsset("test-R", "probation-approve")
}

}
\seealso{
\code{\link{rejectProbation}}, to reject the probational upload.

\code{\link{startUpload}}, to specify probational uploads.
}
\author{
Aaron Lun
}
