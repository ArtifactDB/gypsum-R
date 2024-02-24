#' Search a metadata database
#'
#' Perform a string search on a SQLite database containing metadata from the gypsum backend
#' (see \url{https://github.com/ArtifactDB/bioconductor-metadata-index} for the table structure).
#'
#' @param query List or character vector specifying the query to execute, see Details.
#' @param path String containing a path to a SQLite file, usually obtained via \code{\link{fetchMetadataDatabase}}.
#' @param latest Logical scalar indicating whether to only search for matches within the latest version of each asset.
#' @param include.metadata Logical scalar indicating whether metadata should be returned.
#' @param pid.name String containing the name/alias of the column of the \code{paths} table that contains the path ID.
#'
#' @return 
#' For \code{searchMetadataText}, a data frame specifying the contaning the search results.
#' \itemize{
#' \item The \code{project}, \code{asset} and \code{version} columns contain the identity of the version with matching metadata.
#' \item If \code{include.metadata=TRUE}, a \code{metadata} column is present with the nested metadata for each match.
#' \item If \code{latest=TRUE}, a \code{latest} column is present indicating whether the matching version is the latest for its asset.
#' Otherwise, only the latest version is returned.
#' }
#'
#' For \code{searchMetadataTextFilter}, a list containing \code{where}, a string can be directly used as a WHERE filter condition in a SQL SELECT statement;
#' and \code{parameters}, the parameter bindings to be used in \code{where}.
#' The return value may also be \code{NULL} if the query has no well-defined filter.
#'
#' @author Aaron Lun
#'
#' @details
#' When \code{query} is a list, the SQL filter expression is constructed using the following rules:
#' \itemize{
#' \item If \code{query} has a \code{type} attribute set to \code{"and"}, it represents a boolean AND operation.
#' Each item of \code{query} should be a nested list representing a subquery; all subqueries must match.
#' \item If \code{query} has a \code{type} attribute set to \code{"or"}, it represents a boolean OR operation.
#' Each item of \code{query} should be a nested list representing a subquery; at least one subquery must match.
#' \item If \code{query} does not have a \code{type} attribute, it represents a query on a text string.
#' \code{query} itself should contain a single character vector of length 1, representing the string to be matched.
#' \itemize{
#' \item If \code{query} has a \code{partial} attribute set to \code{TRUE}, the string is directly used for a wildcard match to the token in the database.
#' This expects SQLite's \code{\%} and \code{_} wildcards.
#' \item Otherwise, the string is tokenized and used for equality comparison to tokens in the database.
#' If tokenization yields multiple tokens from the string, all tokens must have matches in the database for the text to be considered as matched.
#' \item Additionally, \code{query} may have a \code{field} attribute, restricting the token matches to a particular metadata field.
#' If this is not present, the search for matching tokens is performed across all available metadata fields.
#' }
#' }
#'
#' Each string is tokenized by converting it to lower case and splitting it on characters that are not Unicode letters/numbers or a dash.
#' We currently do not remove diacritics so these will need to be converted to ASCII by the user. 
#' If a text query involves only non-letter/number/dash characters, the filter will not be well-defined and will be ignored when constructing SQL statements.
#'
#' For convenience, any list inside \code{query} (or indeed, \code{query} itself) may be replaced by a non-empty character vector.
#' A character vector of length 1 is treated as shorthand for a text query with no \code{partial} or \code{field} attributes.
#' A character vector of length greater than 1 is treated as shorthand for an AND operation on text queries for each of the individual strings.
#' 
#' @seealso
#' \code{\link{fetchMetadataDatabase}}, to download and cache the database files.
#'
#' \url{https://github.com/ArtifactDB/bioconductor-metadata-index}, for details on the SQLite file contents.
#' 
#' @examples
#' path <- fetchMetadataDatabase()
#' searchMetadataText(path, c("mouse", "brain"), include.metadata=FALSE)
#'
#' # Now for a slightly more complex query:
#' tissue.query <- list("brain", "pancreas")
#' attr(tissue.query, "type") <- "or"
#' species.query <- list("10090")
#' attr(species.query, "field") <- "taxonomy_id"
#' query <- list(tissue.query, species.query)
#' attr(query, "type") <- "and"
#' searchMetadataText(path, query, include.metadata=FALSE)
#'
#' # Throwing in some wildcards.
#' query <- list("neuro%")
#' attr(query, "partial") <- TRUE
#' searchMetadataText(path, query, include.metadata=FALSE)
#' @export
searchMetadataText <- function(path, query, latest=TRUE, include.metadata=TRUE) {
    where <- searchMetadataTextFilter(query)
    cond <- where$where
    if (is.null(where)) {
        params <- list()
    } else {
        params <- where$parameters
    }

    conn <- DBI::dbConnect(RSQLite::SQLite(), path)
    on.exit(DBI::dbDisconnect(conn))

    stmt <- "SELECT versions.project AS project, versions.asset AS asset, versions.version AS version, path";
    if (include.metadata) {
        stmt <- paste0(stmt, ", json_extract(metadata, '$') AS metadata")
    }
    if (!latest) {
        stmt <- paste0(stmt, ", versions.latest AS latest")
    }
    stmt <- paste0(stmt, " FROM paths LEFT JOIN versions ON paths.vid = versions.vid")

    if (latest) {
        cond <- c(cond, "versions.latest = 1")
    }
    if (length(cond)) {
        stmt <- paste0(stmt, " WHERE ", paste(cond, collapse=" AND "))
    }

    everything <- DBI::dbGetQuery(conn, stmt, params=params)
    if (include.metadata) {
        everything$metadata <- lapply(everything$metadata, fromJSON, simplifyVector=FALSE)
    }
    everything
}

#' @export
#' @rdname searchMetadataText
searchMetadataTextFilter <- function(query, pid.name = 'paths.pid') {
    query <- sanitize_query(query)
    if (is.null(query)) {
        return(NULL)
    }

    env <- new.env()
    env$parameters <- list()
    cond <- build_query(query, pid.name, env)
    list(where=cond, parameters=env$parameters)
}

sanitize_query <- function(query) {
    if (is.character(query)) {
        if (length(query) > 1) {
            query <- as.list(query)
            attr(query, "type") <- "and"
        } else {
            query <- list(query)
        }
    }

    qt <- attr(query, "type")
    if (!is.null(qt)) {
        out <- lapply(query, sanitize_query)

        # Folding all associative operations into a single layer to reduce the nesting of subqueries.
        can.merge <- vapply(out, function(x) identical(attr(x, "type"), qt), TRUE)
        out <- c(unlist(out[can.merge], recursive=FALSE), out[!can.merge])

        keep <- !vapply(out, is.null, FALSE)
        if (!any(keep)) {
            return(NULL)
        }
        out <- out[keep]

        attr(out, "type") <- qt
        return(out)
    }

    if (!is.null(qt)) {
        stop("'type' must be one of 'and', 'or' or absent")
    }
    if (isTRUE(attr(query, "partial"))) {
        return(query)
    }

    text <- strsplit(tolower(query[[1]]), "[^\\p{N}\\p{L}\\p{Co}-]", perl=TRUE)[[1]]
    keep <- vapply(text, nchar, 0L) > 0L
    if (!any(keep)) {
        return(NULL)
    }

    text <- text[keep]
    if (length(text) == 1L) {
        query[[1]] <- text # replacing query to preserve other search information, e.g., the field.
        return(query)
    } 

    output <- vector("list", length(text))
    for (i in seq_along(output)) {
        query[[1]] <- text[[i]]
        output[[i]] <- query
    }
    attr(output, 'type') <- 'and'
    output
}

add_query_parameter <- function(env, value) {
    newname <- paste0("p", length(env$parameters))
    env$parameters[[newname]] <- value
    newname
}

build_query <- function(query, name, env) {
    qt <- attr(query, "type")
    if (is.null(qt)) {
        nt <- add_query_parameter(env, query[[1]])
        if (isTRUE(attr(query, "partial"))) {
            match.str <- paste0("tokens.token LIKE :", nt) 
        } else {
            match.str <- paste0("tokens.token = :", nt) 
        }

        field <- attr(query, "field")
        if (!is.null(field)) {
            nf <- add_query_parameter(env, field)
            return(sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid LEFT JOIN fields ON fields.fid = links.fid WHERE %s AND fields.field = :%s)", name, match.str, nf))
        } else {
            return(sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid WHERE %s)", name, match.str))
        }
    }

    if (qt == "and") {
        out <- lapply(query, build_query, name=name, env=env)
        if (length(out) > 1) {
            return(paste0("(", paste(out, collapse=" AND "), ")"))
        } else {
            return(out[[1]])
        }
    }

    is.text <- vapply(query, function(x) is.null(attr(x, "type")), TRUE)
    out <- character(0)

    # Roll up OR text queries into a single subquery, as we can do that with ORs.
    if (any(is.text)) {
        textual <- query[is.text]
        needs.field <- FALSE

        for (i in seq_along(textual)) {
            current <- textual[[i]]
            nt <- add_query_parameter(env, current[[1]])
            if (isTRUE(attr(current, "partial"))) {
                match.str <- paste0("tokens.token LIKE :", nt) 
            } else {
                match.str <- paste0("tokens.token = :", nt) 
            }

            field <- attr(current, "field")
            if (is.null(field)) {
                textual[[i]] <- match.str
            } else {
                nf <- add_query_parameter(env, field)
                textual[[i]] <- sprintf("(%s AND fields.field = :%s)", match.str, nf)
                needs.field <- TRUE
            }
        }

        textual <- paste(unlist(textual), collapse=" OR ")
        if (needs.field) {
            out <- c(out, sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid LEFT JOIN fields ON fields.fid = links.fid WHERE %s)", name, textual))
        } else {
            out <- c(out, sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid WHERE %s)", name, textual))
        }
    }

    # Everything else needs to be processed separately, I'm afraid.
    if (!all(is.text)) {
        out <- c(out, vapply(query[!is.text], build_query, name=name, env=env, ""))
    }

    if (length(out) > 1) {
        return(paste0("(", paste(out, collapse=" OR "), ")"))
    } else {
        return(out[[1]])
    }
}
