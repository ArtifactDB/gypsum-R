#' Search a metadata database
#'
#' Perform a string search on a SQLite database containing metadata from the gypsum backend
#' (see \url{https://github.com/ArtifactDB/bioconductor-metadata-index}).
#'
#' @param query List or character vector specifying the query to execute, see Details.
#' @param latest Logical scalar indicating whether to only search for matches within the latest version of each asset.
#' Ignored if \code{where.only=TRUE}.
#' @param where.only Logical scalar indicating whether to return the WHERE filter condition.
#' Intended to help developers in assembling their own SQL queries involving a token search.
#' @param include.metadata Logical scalar indicating whether metadata should be returned.
#' Ignored if \code{where.only=TRUE}.
#' @param pid.name String containing the name of the column of the \code{paths} table that contains the path ID.
#' Only used if \code{where.only=TRUE}.
#' @param name,cache,overwrite Further arguments to pass to \code{\link{fetchMetadataDatabase}}.
#'
#' @return 
#' If \code{where.only=FALSE}, a data frame specifying the contaning the results of the query.
#' \itemize{
#' \item The \code{project}, \code{asset} and \code{version} columns contain the identity of the version with matching metadata.
#' \item If \code{include.metadata=TRUE}, a \code{metadata} column is present with the nested metadata for each match.
#' \item If \code{latest=TRUE}, a \code{latest} column is present indicating whether the matching version is the latest for its asset.
#' Otherwise, only the latest version is returned.
#' }
#'
#' If \code{where.only=TRUE}, a list containing \code{where}, a string can be directly used as a WHERE filter condition;
#' and \code{parameters}, the parameter bindings to be used in \code{where}.
#' The return value may also be \code{NULL} if the query has no well-defined filter.
#'
#' @author Aaron Lun
#'
#' @seealso
#' \code{\link{fetchMetadataDatabase}}, to download and cache the database files.
#' 
#' @examples
#' searchMetadataText(c("mouse", "brain"), include.metadata=FALSE)
#'
#' # Now for a slightly more complex query:
#' tissue.query <- list("brain", "pancreas")
#' attr(tissue.query, "type") <- "or"
#' species.query <- list("10090")
#' attr(species.query, "field") <- "taxonomy_id"
#' query <- list(tissue.query, species.query)
#' attr(query, "type") <- "and"
#' searchMetadataText(query, include.metadata=FALSE)
#' @export
searchMetadataText <- function(query, latest=TRUE, include.metadata=TRUE, where.only=FALSE, pid.name="paths.pid", name="bioconductor.sqlite3", cache=cacheDirectory(), overwrite=FALSE) {
    if (!where.only) {
        pid.name <- "paths.pid"
    }

    query <- sanitize_query(query)
    cond <- NULL
    env <- new.env()
    if (!is.null(query)) {
        env$parameters <- list()
        cond <- build_query(query, pid.name, env)
    }

    if (where.only) {
        if (is.null(cond)) {
            return(NULL)
        } else {
            return(list(where=cond, parameters=env$parameters))
        }
    }

    path <- fetchMetadataDatabase(name, cache=cache, overwrite=overwrite)
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

    everything <- DBI::dbGetQuery(conn, stmt, params=env$parameters)
    if (include.metadata) {
        everything$metadata <- lapply(everything$metadata, fromJSON, simplifyVector=FALSE)
    }
    everything
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
        stop("'type' must be one of 'and', 'or' or NULL")
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
        field <- attr(query, "field")
        if (!is.null(field)) {
            nf <- add_query_parameter(env, field)
            return(sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid LEFT JOIN fields ON fields.fid = links.fid WHERE tokens.token = :%s AND fields.field = :%s)", name, nt, nf))
        } else {
            return(sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid WHERE tokens.token = :%s)", name, nt))
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
            field <- attr(current, "field")
            if (is.null(field)) {
                textual[[i]] <- sprintf("tokens.token = :%s", nt)
            } else {
                nf <- add_query_parameter(env, field)
                textual[[i]] <- sprintf("(tokens.token = :%s AND fields.field = :%s)", nt, nf)
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
