\name{gypsum News}
\title{gypsum News}
\encoding{UTF-8}

\section{Version 1.2.0}{\itemize{
\item Switch to \pkg{rappdirs} to choose the cache directory.
This allows re-use of the cache with the equivalent Python client.

\item Improve handling of unexpected errors with non-JSON bodies.

\item Added the \code{unlockProject()} utility for administrators.

\item Renamed \code{defineTextQuery()} to \code{gsc()} (for \dQuote{gypsum search clause}) and generalized it to other fields in the SQLite file.
Specifically, it now can filter based on the path to the metadata document, the project, asset or version names, the identity of the uploader and the timestamp of the upload.

\item Renamed \code{searchMetadataText()} to \code{searchMetadata()}, to reflect the more general nature of the search with the new \code{gsc()} function.
The old name is now soft-deprecated.

\item Renamed \code{searchMetadataTextFilter()} to \code{searchMetadataFilter()}, to reflect the more general nature of the search.
The old name is now soft-deprecated.
Also added arguments to support custom names/aliases for the project, asset, version and path columns.
}}

\section{Version 1.0.1}{\itemize{
\item Read-only operations now use the relevant REST endpoints rather than directly contacting the S3 API.
This avoids a dependency on \pkg{paws.storage} and fixes some strange issues with the request signing.
}}

\section{Version 1.0.0}{\itemize{
\item New \pkg{gypsum} package to interact with the REST API of the same name.
}}
