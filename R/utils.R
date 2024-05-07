#' @importFrom utils URLencode
uenc <- function(x) URLencode(x, reserved=TRUE)

chomp_url <- function(url) sub("/*$", "", url)

.cast_datetime <- function(x) {
    zend <- endsWith(x, "Z")

    if (any(zend)) {
        # strptime doesn't know how to handle 'Z' offsets.
        xz <- x[zend]
        x[zend] <- sprintf("%s+0000", substr(xz, 1L, nchar(xz)-1L))
    }

    if (!all(zend)) {
        # Remove colon in the timezone, which confuses as.POSIXct().
        x[!zend] <- sub(":([0-9]{2})$", "\\1", x[!zend])
    }

    # Remove fractional seconds.
    x <- sub("\\.[0-9]+", "", x)

    as.POSIXct(x, format="%Y-%m-%dT%H:%M:%S%z")
}

sanitize_uploaders <- function(uploaders) {
    for (i in seq_along(uploaders)) {
        current <- uploaders[[i]]
        if ("until" %in% names(current)) {
            uploaders[[i]]$until <- sub("([0-9]{2})$", ":\\1", strftime(current$until, "%Y-%m-%dT%H:%M:%S%z"))
        }
    }
    uploaders
}

#' @import httr2
get_json <- function(path, url) {
    req <- request(paste0(url, "/file/", uenc(path)))
    req <- req_error(req, body = function(res) resp_body_json(res)$reason)
    res <- req_perform(req)
    resp_body_json(res)
}

save_file <- function(path, destination, overwrite, url, error=TRUE) {
    if (overwrite || !file.exists(destination)) {
        dir.create(dirname(destination), recursive=TRUE, showWarnings=FALSE)

        # We use a write-and-rename approach to avoid problems with interrupted
        # downloads that make it seem as if the cache is populated.
        tmp <- tempfile(tmpdir=dirname(destination))
        on.exit(unlink(tmp), add=TRUE, after=FALSE)

        status <- tryCatch(
            {
                req <- request(paste0(url, "/file/", uenc(path)))
                req <- req_error(req, body = function(res) resp_body_json(res)$reason)
                res <- req_perform(req, path=tmp)
            },
            error=function(e) e$message
        )

        if (is.character(status)) {
            if (error) {
                stop("failed to save '", path, "'; ", status)
            } else {
                return(FALSE)
            }
        }

        rename_file(tmp, destination)
    }

    return(TRUE)
}

rename_file <- function(src, dest) {
    if (!file.rename(src, dest) && !file.copy(src, dest)) {
        stop("cannot move temporary file for '", src, "' to its destination '", dest, "'")
    }
}

#' @import httr2
download_and_rename_file <- function(url, dest) {
    # Using the usual write-and-rename strategy.
    tmp <- tempfile(tmpdir=dirname(dest))
    on.exit(unlink(tmp), add=TRUE, after=FALSE)
    req <- request(url)
    req_perform(req, path=tmp)
    rename_file(tmp, dest)
}

BUCKET_CACHE_NAME <- 'bucket'

#' @importFrom jsonlite fromJSON
get_cacheable_json <- function(project, asset, version, path, cache, url, overwrite) {
    bucket_path <- paste(project, asset, version, path, sep="/")
    if (is.null(cache)) {
        get_json(bucket_path, url=url)
    } else {
        out <- file.path(cache, BUCKET_CACHE_NAME, project, asset, version, path)
        acquire_lock(cache, project, asset, version)
        on.exit(release_lock(project, asset, version), add=TRUE, after=FALSE)
        save_file(bucket_path, destination=out, overwrite=overwrite, url=url)
        fromJSON(out, simplifyVector=FALSE)
    }
}

is.locked <- new.env()
is.locked$locks <- list()

#' @importFrom filelock lock
acquire_lock <- function(cache, project, asset, version) {
    key <- paste(project, asset, version, sep="/")
    if (is.null(is.locked$locks[[key]])) {
        lockloc <- file.path(cache, "status", project, asset, version, "LOCK")
        dir.create(dirname(lockloc), recursive=TRUE, showWarnings=FALSE)
        is.locked$locks[[key]] <- lock(lockloc)
    }
}

#' @importFrom filelock unlock
release_lock <- function(project, asset, version) {
    key <- paste(project, asset, version, sep="/")
    if (!is.null(is.locked$locks[[key]])) {
        unlock(is.locked$locks[[key]])
        is.locked$locks[[key]] <- NULL
    }
}

list_for_prefix <- function(prefix, url) {
    url <- paste0(url, "/list")
    if (!is.null(prefix)) {
        url <- paste0(url, "?prefix=", uenc(prefix))
    }

    req <- request(url)
    req <- req_error(req, body = function(res) resp_body_json(res)$reason)
    res <- req_perform(req)
    out <- unlist(resp_body_json(res))

    out <- out[endsWith(out, "/")]
    if (!is.null(prefix)) {
        out <- substr(out, nchar(prefix) + 1L, nchar(out) - 1L)
    } else {
        out <- substr(out, 1L, nchar(out) - 1L)
    }
    out[!startsWith(out, "..")]
}

sanitize_path <- function(x) {
    if (.Platform$OS.type == "windows") {
        x <- gsub("\\\\", "/", x)
    }
    gsub("//+", "/", x)
}
