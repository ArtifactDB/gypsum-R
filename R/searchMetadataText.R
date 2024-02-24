#' Text search on the metadata database
#'
#' Perform a text search on a SQLite database containing metadata from the gypsum backend.
#' This is based on a precomputed tokenization of all string properties in each metadata document;
#' see \url{https://github.com/ArtifactDB/bioconductor-metadata-index} for details.
#'
#' @param query Character vector specifying the query to execute.
#' Alternatively, a \code{gypsum.search.object} produced by \code{defineTextQuery}.
#' @param path String containing a path to a SQLite file, usually obtained via \code{\link{fetchMetadataDatabase}}.
#' @param latest Logical scalar indicating whether to only search for matches within the latest version of each asset.
#' @param include.metadata Logical scalar indicating whether metadata should be returned.
#' @param pid.name String containing the name/alias of the column of the \code{paths} table that contains the path ID.
#' @param text String containing the text to query on.
#' This will be automatically tokenized, see Details.
#' @param field String specifying the name of the metadata field in which to search for \code{text}.
#' If \code{NULL}, the search is performed on all available metadata fields.
#' @param partial Logical scalar indicating whether \code{text} contains SQLite wildcards (\code{\%}, \code{_}) for a partial search.
#' If \code{TRUE}, the wildcards are preserved during tokenization.
#'
#' @return 
#' For \code{searchMetadataText}, a data frame specifying the contaning the search results.
#' \itemize{
#' \item The \code{project}, \code{asset} and \code{version} columns contain the identity of the version with matching metadata.
#' \item The \code{path} column contains the path to the metadata document within the version prefix.
#' \item If \code{include.metadata=TRUE}, a \code{metadata} column is present with the nested metadata for each match.
#' \item If \code{latest=TRUE}, a \code{latest} column is present indicating whether the matching version is the latest for its asset.
#' Otherwise, only the latest version is returned.
#' }
#'
#' For \code{searchMetadataTextFilter}, a list containing \code{where}, a string can be directly used as a WHERE filter condition in a SQL SELECT statement;
#' and \code{parameters}, the parameter bindings to be used in \code{where}.
#' The return value may also be \code{NULL} if the query has no well-defined filter.
#'
#' For \code{defineTextQuery}, a \code{gypsum.search.clause} object that can be used in \code{|} and \code{&} to create more complex queries involving multiple text clauses.
#'
#' @author Aaron Lun
#'
#' @details
#' Each string is tokenized by converting it to lower case and splitting it on characters that are not Unicode letters/numbers or a dash.
#' We currently do not remove diacritics so these will need to be converted to ASCII by the user. 
#' If a text query involves only non-letter/number/dash characters, the filter will not be well-defined and will be ignored when constructing SQL statements.
#'
#' For convenience, a non-empty character vector may be used in \code{query}.
#' A character vector of length 1 is treated as shorthand for a text query with default arguments in \code{defineTextQuery}.
#' A character vector of length greater than 1 is treated as shorthand for an AND operation on default text queries for each of the individual strings.
#' 
#' @seealso
#' \code{\link{fetchMetadataDatabase}}, to download and cache the database files.
#'
#' \url{https://github.com/ArtifactDB/bioconductor-metadata-index}, for details on the SQLite file contents and table structure.
#' 
#' @examples
#' path <- fetchMetadataDatabase()
#' searchMetadataText(path, c("mouse", "brain"), include.metadata=FALSE)
#'
#' # Now for a slightly more complex query:
#' is.mouse <- defineTextQuery("10090", field="taxonomy_id")
#' query <- (defineTextQuery("brain") | defineTextQuery("pancreas")) & is.mouse
#' searchMetadataText(path, query, include.metadata=FALSE)
#'
#' # Throwing in some wildcards.
#' has.neuro <- defineTextQuery("Neuro%", partial=TRUE)
#' searchMetadataText(path, has.neuro, include.metadata=FALSE)
#'
#' @aliases
#' gypsum.search.clause
#' Ops.gypsum.search.clause
#' @export
searchMetadataText <- function(path, query, latest=TRUE, include.metadata=TRUE) {
    where <- searchMetadataTextFilter(query)
    cond <- where$where
    params <- where$parameters

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

    if (is.null(params)) {
        everything <- DBI::dbGetQuery(conn, stmt)
    } else {
        everything <- DBI::dbGetQuery(conn, stmt, params=params)
    }
    if (include.metadata) {
        everything$metadata <- lapply(everything$metadata, fromJSON, simplifyVector=FALSE)
    }
    everything
}

#' @export
#' @rdname searchMetadataText
defineTextQuery <- function(text, field=NULL, partial=FALSE) {
    output <- list(type="text", text=text, field=field, partial=partial)
    class(output) <- "gypsum.search.clause"
    output
}

#' @export
Ops.gypsum.search.clause <- function(e1, e2) {
    if (.Generic == "&") {
        output <- list(type="and", children=list(e1, e2))
    } else if (.Generic == "|") {
        output <- list(type="or", children=list(e1, e2))
    } else {
        stop("unsupported generic '", .Generic, "' for 'gypsum.search.clause'")
    }
    class(output) <- "gypsum.search.clause"
    output
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
            query <- list(type="and", children=lapply(query, defineTextQuery))
            class(query) <- "gypsum.search.clause"
        } else if (length(query) == 1L) {
            query <- defineTextQuery(query)
        } else {
            stop("character vector must be non-empty")
        }
    }

    qt <- query$type
    if (qt != "text") {
        rechildren <- lapply(query$children, sanitize_query)

        keep <- !vapply(rechildren, is.null, FALSE)
        if (!any(keep)) {
            return(NULL)
        }
        if (!all(keep)) {
            rechildren <- rechildren[keep]
        }
        if (length(rechildren) == 1L) {
            return(rechildren[[1]])
        }

        # Collapse AND/ORs into a single layer to reduce nesting of subqueries.
        can.merge <- vapply(rechildren, function(x) x$type, "") == qt
        if (any(can.merge)) {
            grandchildren <- lapply(rechildren[can.merge], function(x) x$children)
            rechildren <- c(unlist(grandchildren, recursive=FALSE), rechildren[!can.merge])
        }

        output <- list(type=qt, children=rechildren)
        class(output) <- "gypsum.search.clause"
        return(output)
    }

    extras <- ""
    if (isTRUE(query$partial)) {
        extras <- "%_"
    }

    text <- strsplit(tolower(query$text), sprintf("[^\\p{N}\\p{L}\\p{Co}%s-]", extras), perl=TRUE)[[1]]
    keep <- vapply(text, nchar, 0L) > 0L
    if (!any(keep)) {
        return(NULL)
    }

    children <- lapply(text[keep], defineTextQuery, field=query$field, partial=isTRUE(query$partial))
    if (length(children) == 1L) {
        return(children[[1]])
    }

    output <- list(type="and", children=children)
    class(output) <- "gypsum.search.clause"
    output 
}

add_query_parameter <- function(env, value) {
    newname <- paste0("p", length(env$parameters))
    env$parameters[[newname]] <- value
    newname
}

build_query <- function(query, name, env) {
    qt <- query$type
    if (qt == "text") {
        nt <- add_query_parameter(env, query$text)
        if (isTRUE(query$partial)) {
            match.str <- paste0("tokens.token LIKE :", nt) 
        } else {
            match.str <- paste0("tokens.token = :", nt) 
        }

        field <- query$field
        if (!is.null(field)) {
            nf <- add_query_parameter(env, field)
            return(sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid LEFT JOIN fields ON fields.fid = links.fid WHERE %s AND fields.field = :%s)", name, match.str, nf))
        } else {
            return(sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid WHERE %s)", name, match.str))
        }
    }

    if (qt == "and") {
        out <- lapply(query$children, build_query, name=name, env=env)
        return(paste0("(", paste(out, collapse=" AND "), ")"))
    }

    is.text <- vapply(query$children, function(x) query$type, "") == "text"
    out <- character(0)

    # Collapse text children into a single subquery.
    if (any(is.text)) {
        textual <- query$children[is.text]
        needs.field <- FALSE

        for (i in seq_along(textual)) {
            current <- textual[[i]]
            nt <- add_query_parameter(env, current[[1]])
            if (isTRUE(current$partial)) {
                match.str <- paste0("tokens.token LIKE :", nt) 
            } else {
                match.str <- paste0("tokens.token = :", nt) 
            }

            field <- current$field
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

    # All non-text children to be processed as separate subqueries, I'm afraid.
    if (!all(is.text)) {
        out <- c(out, vapply(query$children[!is.text], build_query, name=name, env=env, ""))
    }
    paste0("(", paste(out, collapse=" OR "), ")")
}
