#' Search the metadata database
#'
#' Search a SQLite database containing metadata from the gypsum backend.
#' This is based on a precomputed tokenization of all string properties in each metadata document;
#' see \url{https://github.com/ArtifactDB/bioconductor-metadata-index} for details.
#'
#' @param query Character vector specifying the query to execute.
#' Alternatively, a \code{gypsum.search.clause} produced by \code{gsc}.
#' @param path For \code{searchMetadata}, a string containing a path to a SQLite file, usually obtained via \code{\link{fetchMetadataDatabase}}.
#' 
#' For \code{gsc}, the suffix of the object key of the metadata document, 
#' i.e., the relative \dQuote{path} to the metadata file inside the version's \dQuote{directory}.
#' This may be missing as long as other arguments are supplied to \code{gsc}.
#' @param latest Logical scalar indicating whether to only search for matches within the latest version of each asset.
#' @param include.metadata Logical scalar indicating whether metadata should be returned.
#' @param pid.name String containing the name/alias of the column of the \code{paths} table that contains the path ID.
#' @param project.name String containing the name/alias of the column of the \code{versions} table that contains the project name.
#' @param asset.name String containing the name/alias of the column of the \code{versions} table that contains the asset name.
#' @param version.name String containing the name/alias of the column of the \code{versions} table that contains the version name.
#' @param path.name String containing the name/alias of the column of the \code{paths} table that contains the path name.
#' @param user.name String containing the name/alias of the column of the \code{versions} table that contains the user ID of the uploader.
#' @param time.name String containing the name/alias of the column of the \code{versions} table that contains the upload time.
#' @param text String containing the text to query on.
#' This will be automatically tokenized, see Details.
#' This may be missing as long as other arguments are supplied to \code{gsc}.
#' @param project String containing the name of the project.
#' This may be missing as long as other arguments are supplied to \code{gsc}.
#' @param asset String containing the name of the asset.
#' This may be missing as long as other arguments are supplied to \code{gsc}.
#' @param version String containing the name of the version.
#' This may be missing as long as other arguments are supplied to \code{gsc}.
#' @param user String containing the user ID of the uploader.
#' This may be missing as long as other arguments are supplied to \code{gsc}.
#' @param time Number specifying the Unix timestamp (in seconds) at which the upload was finished.
#' This may be missing as long as other arguments are supplied to \code{gsc}.
#' @param field String specifying the name of the metadata field in which to search for \code{text}.
#' If \code{NULL}, the search is performed on all available metadata fields.
#' @param partial For \code{gsc}, a logical scalar indicating whether \code{text}, \code{project}, \code{asset}, \code{version}, \code{path} or \code{user} 
#' contains SQLite wildcards (\code{\%}, \code{_}) for a partial search.
#' For \code{text}, setting \code{partial=TRUE} also ensures that the wildcards are preserved during tokenization.
#' @param after Logical scalar indicating whether to search for documents that were uploaded after \code{time}.
#' If \code{FALSE}, the search will instead consider documents that were uploaded at or before \code{time}.
#'
#' @return 
#' For \code{searchMetadata}, a data frame specifying the contaning the search results.
#' \itemize{
#' \item The \code{project}, \code{asset} and \code{version} columns specify the version of the project asset with matching metadata.
#' \item The \code{path} column contains the suffix of the object key of the metadata document,
#' i.e., the relative \dQuote{path} within the version's \dQuote{directory} to the metadata document.
#' The full object key of the document inside the bucket is defined as \code{{project}/{asset}/{version}/{path}}.
#' \item The \code{user} column contains the identity of the uploading user.
#' \item The \code{time} column contains the time of the upload.
#' \item If \code{include.metadata=TRUE}, a \code{metadata} column is present with the nested metadata for each match.
#' \item If \code{latest=TRUE}, a \code{latest} column is present indicating whether the matching version is the latest for its asset.
#' Otherwise, only the latest version is returned.
#' }
#'
#' For \code{searchMetadataFilter}, a list containing \code{where}, a string can be directly used as a WHERE filter condition in a SQL SELECT statement;
#' and \code{parameters}, the parameter bindings to be used in \code{where}.
#' The return value may also be \code{NULL} if the query has no well-defined filter.
#'
#' For \code{gsc}, a gypsum.search.clause object that can be used in \code{|}, \code{&} and \code{!} to create more complex queries involving multiple clauses.
#'
#' @author Aaron Lun
#'
#' @details
#' Each string is tokenized by converting it to lower case and splitting it on characters that are not Unicode letters/numbers or a dash.
#' We currently do not remove diacritics so these will need to be converted to ASCII by the user. 
#' If a text query involves only non-letter/number/dash characters, the filter will not be well-defined and will be ignored when constructing SQL statements.
#'
#' For convenience, a non-empty character vector may be used in \code{query}.
#' A character vector of length 1 is treated as shorthand for a text query with default arguments in \code{gsc}.
#' A character vector of length greater than 1 is treated as shorthand for an AND operation on default text queries for each of the individual strings.
#' 
#' @seealso
#' \code{\link{fetchMetadataDatabase}}, to download and cache the database files.
#'
#' \url{https://github.com/ArtifactDB/bioconductor-metadata-index}, for details on the SQLite file contents and table structure.
#' 
#' @examples
#' path <- fetchMetadataDatabase()
#' searchMetadata(path, c("mouse", "brain"), include.metadata=FALSE)
#'
#' # Now for a slightly more complex query:
#' is.mouse <- gsc("10090", field="taxonomy_id")
#' query <- (gsc("brain") | gsc("pancreas")) & is.mouse
#' searchMetadata(path, query, include.metadata=FALSE)
#'
#' # Throwing in some wildcards.
#' has.neuro <- gsc("Neuro%", partial=TRUE)
#' searchMetadata(path, has.neuro, include.metadata=FALSE)
#'
#' # We can also query other properties.
#' datasets <- gsc(project="scRNAseq") & gsc(asset="l%", partial=TRUE)
#' searchMetadata(path, datasets, include.metadata=FALSE)
#'
#' @aliases
#' gypsum.search.clause
#' Ops.gypsum.search.clause
#' searchMetadataText
#' defineTextQuery
#' searchMetadataTextFilter
#'
#' @export
searchMetadata <- function(path, query, latest=TRUE, include.metadata=TRUE) {
    where <- searchMetadataFilter(query)
    cond <- where$where
    params <- where$parameters

    conn <- DBI::dbConnect(RSQLite::SQLite(), path)
    on.exit(DBI::dbDisconnect(conn), add=TRUE, after=FALSE)
    version <- DBI::dbGetQuery(conn, "PRAGMA user_version")[,1]

    stmt <- "SELECT versions.project AS project, versions.asset AS asset, versions.version AS version, path"
    if (version >= 1001000) {
        stmt <- paste0(stmt, ", versions.user AS user, versions.time AS time")
    }
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
    if (version >= 1001000) {
        everything$time <- as.POSIXct(everything$time)
    }

    everything
}

#' @export
searchMetadataText <- function(...) {
    searchMetadata(...) # Soft-deprecated.
}

#' @export
#' @rdname searchMetadata
gsc <- function(text=NULL, project=NULL, asset=NULL, version=NULL, path=NULL, user=NULL, time=NULL, field=NULL, partial=FALSE, after=TRUE) {
    collected <- list()

    if (!is.null(text)) {
        collected <- c(collected, list(list(type="text", text=text, field=field, partial=partial)))
    }

    if (!is.null(project)) {
        collected <- c(collected, list(list(type = "project", project = project, partial = partial)))
    }

    if (!is.null(asset)) {
        collected <- c(collected, list(list(type = "asset", asset = asset, partial = partial)))
    }

    if (!is.null(version)) {
        collected <- c(collected, list(list(type = "version", version = version, partial = partial)))
    }

    if (!is.null(path)) {
        collected <- c(collected, list(list(type = "path", path = path, partial = partial)))
    }

    if (!is.null(user)) {
        collected <- c(collected, list(list(type = "user", user = user, partial = partial)))
    }

    if (!is.null(time)) {
        collected <- c(collected, list(list(type = "time", time = as.double(time), after=after)))
    }

    for (i in seq_along(collected)) {
        class(collected[[i]]) <- "gypsum.search.clause"
    }

    if (length(collected) == 0) {
        stop("at least one of 'text', 'project', 'asset', 'version', 'path', 'user' or 'time' must be specified")
    } else if (length(collected) == 1) {
        collected[[1]]
    } else {
        output <- list(type = "and", children = collected)
        class(output) <- "gypsum.search.clause"
        output
    }
}

#' @export
defineTextQuery <- function(text, field=NULL, partial=FALSE) {
    gsc(text, field=field, partial=partial) # Soft-deprecated.
}

#' @export
Ops.gypsum.search.clause <- function(e1, e2) {
    if (.Generic == "&") {
        output <- list(type="and", children=list(e1, e2))
    } else if (.Generic == "|") {
        output <- list(type="or", children=list(e1, e2))
    } else if (.Generic == "!") {
        output <- list(type="not", child=e1) 
    } else {
        stop("unsupported generic '", .Generic, "' for 'gypsum.search.clause'")
    }
    class(output) <- "gypsum.search.clause"
    output
}

#' @export
#' @rdname searchMetadata
searchMetadataFilter <- function(
    query,
    pid.name = 'paths.pid', 
    project.name="versions.project",
    asset.name="versions.asset",
    version.name="versions.version",
    path.name="paths.path",
    user.name="versions.user",
    time.name="versions.time")
{
    query <- sanitize_query(query)
    if (is.null(query)) {
        return(NULL)
    }

    names <- list(
        pid=pid.name,
        project=project.name,
        asset=asset.name,
        version=version.name,
        path=path.name,
        user=user.name,
        time=time.name
    )

    env <- new.env()
    env$parameters <- list()
    cond <- build_query(query, names, env)
    list(where=cond, parameters=env$parameters)
}

#' @export
searchMetadataTextFilter <- function(...) {
    searchMetadataFilter(...) # Soft-deprecated.
}

sanitize_query <- function(query) {
    if (is.character(query)) {
        if (length(query) > 1) {
            query <- list(type="and", children=lapply(query, gsc))
            class(query) <- "gypsum.search.clause"
        } else if (length(query) == 1L) {
            query <- gsc(query)
        } else {
            stop("character vector must be non-empty")
        }
    }

    qt <- query$type
    if (qt %in% c("project", "asset", "version", "path", "user", "time")) {
        return(query)
    }

    if (qt == "not") {
        child <- sanitize_query(query$child)
        if (is.null(child)) {
            return(NULL)
        } 
        return(list(type="not", child=child))
    }

    if (qt == "and" || qt == "or") {
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

add_token_search_clause <- function(query, env) {
    nt <- add_query_parameter(env, query$text)
    if (isTRUE(query$partial)) {
        paste0("tokens.token LIKE :", nt) 
    } else {
        paste0("tokens.token = :", nt) 
    }
}

setup_token_search_subquery_basic <- function(names, where) {
    sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid WHERE %s)", names$pid, where)
}

setup_token_search_subquery_with_field <- function(names, where) {
    sprintf("%s IN (SELECT pid from links LEFT JOIN tokens ON tokens.tid = links.tid LEFT JOIN fields ON fields.fid = links.fid WHERE %s)", names$pid, where)
}

build_query <- function(query, names, env) {
    qt <- query$type

    if (qt == "text") {
        match.str <- add_token_search_clause(query, env)
        field <- query$field
        if (!is.null(field)) {
            nf <- add_query_parameter(env, field)
            return(setup_token_search_subquery_with_field(names, sprintf("fields.field = :%s AND %s", nf, match.str)))
        } else {
            return(setup_token_search_subquery_basic(names, match.str))
        }
    }

    if (qt %in% c("project", "asset", "version", "path", "user")) {
        nt <- add_query_parameter(env, query[[qt]])
        db.name <- names[[qt]]
        if (isTRUE(query$partial)) {
            return(sprintf("%s LIKE :%s", db.name, nt))
        } else {
            return(sprintf("%s = :%s", db.name, nt))
        }
    }

    if (qt == "time") {
        nt <- add_query_parameter(env, query[[qt]])
        db.name <- names[[qt]]
        op <- if (isTRUE(query$after)) ">" else "<="
        return(sprintf("%s %s :%s", db.name, op, nt))
    }

    if (qt == "not") {
        return(paste0("NOT ", build_query(query$child, names, env)))
    }

    if (qt == "and") {
        out <- lapply(query$children, build_query, name=names, env=env)
        return(paste0("(", paste(out, collapse=" AND "), ")"))
    }

    # Collapse text children into a single subquery.
    is.text <- vapply(query$children, function(x) x$type, "") == "text"
    out <- character(0)
    if (any(is.text)) {
        textual <- query$children[is.text]
        needs.field <- FALSE

        for (i in seq_along(textual)) {
            current <- textual[[i]]
            match.str <- add_token_search_clause(current, env)
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
            out <- c(out, setup_token_search_subquery_with_field(names, textual))
        } else {
            out <- c(out, setup_token_search_subquery_basic(names, textual))
        }
    }

    # All non-text children to be processed as separate subqueries, I'm afraid.
    if (!all(is.text)) {
        out <- c(out, vapply(query$children[!is.text], build_query, names=names, env=env, ""))
    }
    paste0("(", paste(out, collapse=" OR "), ")")
}
