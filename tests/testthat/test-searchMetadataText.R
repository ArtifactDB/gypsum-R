# This tests the searchMetadataText function.
# library(testthat); library(gypsum); source("test-searchMetadataText.R")

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
        latest = c(FALSE, FALSE, TRUE)
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

test_that("searchMetadataText works for text searches", {
    out <- searchMetadataText(tmp, c("mikoto"), include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, "mikoto.txt")

    # Tokenization works correctly.
    out <- searchMetadataText(tmp, c(" kuroko "), include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("kuroko.txt"))
    out <- searchMetadataText(tmp, c("TOKIWADAI"), include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "mitsuko.txt", "kuroko.txt", "misaki.txt"))

    # Partial matching works correctly.
    query <- defineTextQuery("Mi%", partial=TRUE)
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "mitsuko.txt", "misaki.txt"))

    # Field-specific matching works correctly.
    query <- defineTextQuery("sa%", partial=TRUE, field="last_name")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("ruiko.txt"))
})

test_that("searchMetadataText works for AND searches", {
    # AND automatically happens upon tokenization.
    out <- searchMetadataText(tmp, c("sakugawa judgement"), include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, "kazari.txt")

    # We can also be more explicit.
    query <- defineTextQuery("rank") & defineTextQuery("male")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, "accelerator.txt")

    # Nested ANDs are handled properly.
    query <- (defineTextQuery("s%", partial=TRUE) & defineTextQuery("tokiwadai")) & defineTextQuery("judgement")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, "kuroko.txt")
})

test_that("searchMetadataText works for OR searches", {
    query <- defineTextQuery("uiharu") | defineTextQuery("rank")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "misaki.txt", "kazari.txt", "accelerator.txt"))

    # ORs work correctly with partial matches.
    query <- defineTextQuery("judgement") | defineTextQuery("Mi%", partial=TRUE)
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "mitsuko.txt", "kuroko.txt", "misaki.txt", "kazari.txt"))

    # ORs work correctly with field matches.
    query <- defineTextQuery("mi%", partial=TRUE, field="last_name") | defineTextQuery("judgement")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "kuroko.txt", "kazari.txt"))

    # Nested ORs are collapsed properly.
    query <- (defineTextQuery("teleport") | defineTextQuery("aerohand")) | defineTextQuery("mental")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mitsuko.txt", "kuroko.txt", "misaki.txt"))
})

test_that("searchMetadataText works with combined AND and OR searches", {
    # OR that contains an AND.
    query <- (defineTextQuery("judgement") & defineTextQuery("sakugawa")) | defineTextQuery("aerohand") | defineTextQuery("vector")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mitsuko.txt", "kazari.txt", "accelerator.txt"))

    # OR that contains multiple ANDs.
    query <- (defineTextQuery("judgement") & defineTextQuery("sakugawa")) | (defineTextQuery("female") & defineTextQuery("rank"))
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("mikoto.txt", "misaki.txt", "kazari.txt"))

    # AND that contains an OR.
    query <- defineTextQuery("rank") & (defineTextQuery("shokuhou") | defineTextQuery("kongou"))
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("misaki.txt"))

    # AND that contains multiple ORs.
    query <- (defineTextQuery("rank") | defineTextQuery("judgement")) & (defineTextQuery("male") | defineTextQuery("teleport"))
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(out$path, c("kuroko.txt", "accelerator.txt"))
})

test_that("searchMetadataText works with ill-defined filters", {
    # We return everything.
    out <- searchMetadataText(tmp, "     ", include.metadata=FALSE, latest=FALSE)
    expect_identical(nrow(out), 7L)

    # Ill-defined filters are ignored in boolean operations.
    query <- defineTextQuery("female") & defineTextQuery("    ")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(nrow(out), 6L)

    query <- defineTextQuery("male") | defineTextQuery("    ")
    out <- searchMetadataText(tmp, query, include.metadata=FALSE, latest=FALSE)
    expect_identical(nrow(out), 1L)
})

test_that("searchMetadataText respects the other output options", {
    out <- searchMetadataText(tmp, c("female"))
    expect_identical(out$path, c("kuroko.txt", "kazari.txt"))
    expect_identical(out$path, paste0(vapply(out$metadata, function(x) x$first_name, ""), ".txt"))
})

