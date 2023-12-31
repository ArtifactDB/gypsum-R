# This checks the metadata-based utilities.
# library(testthat); library(gypsum); source("test-metadata.R")

cache <- tempfile()

test_that("fetchMetadataSchema works as expected", {
    path <- fetchMetadataSchema(cache=cache)
    expect_error(jsonlite::fromJSON(path), NA)

    # Uses the cache.
    writeLines(con=path, "FOO")
    path2 <- fetchMetadataSchema(cache=cache)
    expect_identical(path, path2)
    expect_identical(readLines(path), "FOO")

    # Unless we overwrite it.
    man <- fetchMetadataSchema(cache=cache, overwrite=TRUE)
    expect_error(jsonlite::fromJSON(path), NA)
})

test_that("fetchMetadataDatabase works as expected", {
    path <- fetchMetadataDatabase(cache=cache)
    expect_true(file.info(path)$size > 0)
    expect_type(gypsum:::last_check$req_time, "double")
    expect_true(!is.na(gypsum:::last_check$req_time))
    expect_type(gypsum:::last_check$mod_time, "double")
    expect_true(!is.na(gypsum:::last_check$mod_time))

    # Uses the cache.
    writeLines(con=path, "FOO")
    path2 <- fetchMetadataDatabase(cache=cache)
    expect_identical(path, path2)
    expect_identical(readLines(path), "FOO")
})

test_that("validateMetadata works as expected", {
    metadata <- list(                           
        title="Fatherhood",
        description="Luke ich bin dein Vater.",
        sources=list(
           list(provider="GEO", id="GSE12345")
        ),
        taxonomy_id=list("9606"),
        genome=list("GRCm38"),
        maintainer_name="Darth Vader",
        maintainer_email="vader@empire.gov",
        bioconductor_version="3.10"
    )

    schema <- fetchMetadataSchema(cache=cache)
    expect_error(validateMetadata(metadata, schema), NA)
    expect_error(validateMetadata(jsonlite::toJSON(metadata, auto_unbox=TRUE), schema), NA) # stringification works as expected.

    metadata$bioconductor_version <- NULL
    expect_error(validateMetadata(metadata, schema), "bioconductor_version")
})
