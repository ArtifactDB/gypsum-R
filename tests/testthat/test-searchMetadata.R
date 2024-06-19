# This tests the searchMetadata function.
# library(testthat); library(gypsum); source("test-searchMetadata.R")

library(DBI)
library(RSQLite)

tmp <- tempfile(fileext=".sqlite3")
(function() {
    conn <- dbConnect(SQLite(), tmp)
    on.exit(dbDisconnect(conn))

    dbWriteTable(conn, "versions", data.frame(
        vid = 1:3,
        project = "foo",
        asset = "bar",
        version = as.character(1:3),
        latest = c(FALSE, FALSE, TRUE),
        user = c("foo", "bar", "stuff"),
        time = 1:3
    ))

    metadata <- list(
        list(first_name="mikoto", last_name="misaka", school="tokiwadai", ability="railgun", gender="female", comment="rank 3"),
        list(first_name="mitsuko", last_name="kongou", school="tokiwadai", ability="aerohand", gender="female"),
        list(first_name="kuroko", last_name="shirai", school="tokiwadai", ability="teleport", gender="female", affiliation="judgement"),
        list(first_name="misaki", last_name="shokuhou", school="tokiwadai", ability="mental out", gender="female", comment="rank 5"),
        list(first_name="ruiko", last_name="saten", school="sakugawa", gender="female"),
        list(first_name="kazari", last_name="uiharu", school="sakugawa", gender="female", affiliation="judgement"),
        list(first_name="accelerator", ability="vector manipulation", gender="male", comment="rank 1")
    )

    dbWriteTable(conn, "paths", data.frame(
        pid = seq_along(metadata),
        vid = rep(1:3, length.out=length(metadata)),
        path = paste0(vapply(metadata, function(x) x$first_name, ""), ".txt"),
        metadata = vapply(metadata, jsonlite::toJSON, auto_unbox=TRUE, "")
    ))

    all.tokens <- unlist(strsplit(unique(unlist(metadata, use.names=FALSE)), " "))
    dbWriteTable(conn, "tokens", data.frame(tid = seq_along(all.tokens), token = all.tokens))

    all.fields <- unique(unlist(lapply(metadata, names)))
    dbWriteTable(conn, "fields", data.frame(fid = seq_along(all.fields), field = all.fields))

    links <- list(pid = integer(0), fid = integer(0), tid = integer(0))
    for (i in seq_along(metadata)) {
        my.fields <- names(metadata[[i]])
        my.tokens <- lapply(metadata[[i]], function(x) unique(strsplit(x, " ")[[1]]))
        my.fields <- rep(my.fields, lengths(my.tokens))
        my.tokens <- unlist(my.tokens, use.names=FALSE)
        links$pid <- c(links$pid, rep(i, length(my.tokens)))
        links$fid <- c(links$fid, match(my.fields, all.fields))
        links$tid <- c(links$tid, match(my.tokens, all.tokens))
    }
    dbWriteTable(conn, "links", data.frame(links))
})()

test_that("searchMetadata works for text searches", {
    out <- searchMetadata(tmp, c("mikoto"), include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, "mikoto.txt")

    # Tokenization works correctly.
    out <- searchMetadata(tmp, c(" kuroko "), include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("kuroko.txt"))
    out <- searchMetadata(tmp, c("TOKIWADAI"), include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "mitsuko.txt", "kuroko.txt", "misaki.txt"))

    # Partial matching works correctly.
    query <- gsc("Mi%", partial=TRUE)
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "mitsuko.txt", "misaki.txt"))

    # Field-specific matching works correctly.
    query <- gsc("sa%", partial=TRUE, field="last_name")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("ruiko.txt"))
})

test_that("searchMetadata works for AND searches", {
    # AND automatically happens upon tokenization.
    out <- searchMetadata(tmp, c("sakugawa judgement"), include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, "kazari.txt")

    # We can also be more explicit.
    query <- gsc("rank") & gsc("male")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, "accelerator.txt")

    # Nested ANDs are handled properly.
    query <- (gsc("s%", partial=TRUE) & gsc("tokiwadai")) & gsc("judgement")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, "kuroko.txt")
})

test_that("searchMetadata works for OR searches", {
    query <- gsc("uiharu") | gsc("rank")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "misaki.txt", "kazari.txt", "accelerator.txt"))

    # ORs work correctly with partial matches.
    query <- gsc("judgement") | gsc("Mi%", partial=TRUE)
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "mitsuko.txt", "kuroko.txt", "misaki.txt", "kazari.txt"))

    # ORs work correctly with field matches.
    query <- gsc("mi%", partial=TRUE, field="last_name") | gsc("judgement")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "kuroko.txt", "kazari.txt"))

    # Nested ORs are collapsed properly.
    query <- (gsc("teleport") | gsc("aerohand")) | gsc("mental")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mitsuko.txt", "kuroko.txt", "misaki.txt"))

    query <- (gsc("%sa%", field="school", partial=TRUE) | gsc("mental"))
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("misaki.txt", "ruiko.txt", "kazari.txt"))
})

test_that("searchMetadata works with combined AND and OR searches", {
    # OR that contains an AND.
    query <- (gsc("judgement") & gsc("sakugawa")) | gsc("aerohand") | gsc("vector")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mitsuko.txt", "kazari.txt", "accelerator.txt"))

    # OR that contains multiple ANDs.
    query <- (gsc("judgement") & gsc("sakugawa")) | (gsc("female") & gsc("rank"))
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "misaki.txt", "kazari.txt"))

    # AND that contains an OR.
    query <- gsc("rank") & (gsc("shokuhou") | gsc("kongou"))
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("misaki.txt"))

    # AND that contains multiple ORs.
    query <- (gsc("rank") | gsc("judgement")) & (gsc("male") | gsc("teleport"))
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("kuroko.txt", "accelerator.txt"))
})

test_that("searchMetadata works for NOT searches", {
    query <- !gsc("uiharu") 
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "mitsuko.txt", "kuroko.txt", "misaki.txt", "ruiko.txt", "accelerator.txt"))

    query <- !gsc("mi%", partial=TRUE) 
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("kuroko.txt", "ruiko.txt", "kazari.txt", "accelerator.txt"))

    query <- !(gsc("uiharu") | gsc("rank"))
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mitsuko.txt", "kuroko.txt", "ruiko.txt"))

    query <- gsc("rank") & !gsc("tokiwadai")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("accelerator.txt"))
})

test_that("searchMetadata works for non-text-based searches", {
    query <- gsc(project="foo", asset="bar")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(sort(out$path), sort(c("mikoto.txt", "mitsuko.txt", "kuroko.txt", "misaki.txt", "ruiko.txt", "kazari.txt", "accelerator.txt"))) 

    query <- gsc(version="2")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mitsuko.txt", "ruiko.txt"))

    query <- gsc(path="%ko_txt", partial=TRUE)
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mitsuko.txt", "kuroko.txt", "ruiko.txt"))

    query <- gsc(user="foo")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(sort(out$path), sort(c("mikoto.txt", "misaki.txt", "accelerator.txt"))) 

    query <- gsc(time=2, after=TRUE) & gsc(time=3, after=FALSE)
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(sort(out$path), sort(c("kuroko.txt", "kazari.txt")))

    # Combines with the other searches.
    query <- gsc(path="kuroko.txt") | gsc("railgun")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "kuroko.txt"))
})

test_that("searchMetadata works with ill-defined filters", {
    # We return everything.
    out <- searchMetadata(tmp, "     ", include.metadata=FALSE, latest=FALSE)
    expect_identical(nrow(out), 7L)

    # Ill-defined filters are ignored in boolean operations.
    query <- gsc("female") & gsc("    ")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(nrow(out), 6L)

    query <- gsc("male") | gsc("    ")
    out <- searchMetadata(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(nrow(out), 1L)
})

test_that("searchMetadata respects the other output options", {
    out <- searchMetadata(tmp, c("female"))
    expect_identical(out$path, c("kuroko.txt", "kazari.txt"))
    expect_identical(out$path, paste0(vapply(out$metadata, function(x) x$first_name, ""), ".txt"))
})

