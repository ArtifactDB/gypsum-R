\name{gypsum News}
\title{gypsum News}
\encoding{UTF-8}

\section{Version 1.2.0}{\itemize{
\item Switch to \pkg{rappdirs} to choose the cache directory.
This allows re-use of the cache with the equivalent Python client.

\item Improve handling of unexpected errors with non-JSON bodies.

\item Added the \code{unlockProject()} utility for administrators.

\item Added \code{definePathQuery()} to allow construction of queries that use the path to the metadata document, as well as the project, asset or version names.

\item Extended \code{searchMetadataTextFilter()} to support custom names/aliases for the project, asset, version and path columns.
}}

\section{Version 1.0.1}{\itemize{
\item Read-only operations now use the relevant REST endpoints rather than directly contacting the S3 API.
This avoids a dependency on \pkg{paws.storage} and fixes some strange issues with the request signing.
}}

\section{Version 1.0.0}{\itemize{
\item New \pkg{gypsum} package to interact with the REST API of the same name.
}}
