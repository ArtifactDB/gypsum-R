# This checks the cloneVersion function.
# library(testthat); library(gypsum); source("test-prepareDirectoryUpload.R")

test_that("prepareDirectoryUpload works as expected", {
    cache <- tempfile()
    dest <- tempfile()
    cloneVersion("test-R", "basic", "v1", destination=dest, cache=cache)
    write(file=file.path(dest, "heanna"), "sumire")

    prepped <- prepareDirectoryUpload(dest, cache=cache)
    if (.Platform$OS.type == "windows") { # because windows doesn't have guaranteed support for symbolic links.
        expect_identical(prepped$files, c("blah.txt", "foo/bar.txt", "heanna"))
        expect_identical(nrow(prepped$links), 0L)
    } else {
        expect_identical(prepped$files, "heanna")
        expect_identical(prepped$links$from.path, c("blah.txt", "foo/bar.txt"))
        expect_identical(prepped$links$to.project, rep("test-R", 2))
        expect_identical(prepped$links$to.asset, rep("basic", 2))
        expect_identical(prepped$links$to.version, rep("v1", 2))
        expect_identical(prepped$links$to.path, c("blah.txt", "foo/bar.txt"))
    }

    prepped <- prepareDirectoryUpload(dest, cache=cache, links="never")
    expect_identical(prepped$files, c("blah.txt", "foo/bar.txt", "heanna"))
    expect_identical(nrow(prepped$links), 0L)
})

test_that("prepareDirectoryUpload works with some odd things", {
    skip_on_os("windows") # giving up on flaky support for symbolic links on Windows.

    cache <- tempfile()
    dest <- tempfile()
    cloneVersion("test-R", "basic", "v1", destination=dest, cache=cache)

    write(file=file.path(dest, "..check"), "stuff") # check that we ignore hidden files.
    random <- tempfile()
    file.symlink(random, file.path(dest, "arkansas")) # create a dangling link to a non-cache file.

    expect_error(prepareDirectoryUpload(dest, cache=cache, links="always"), "failed to convert")
    expect_error(prepareDirectoryUpload(dest, cache=cache, links="never"), "cannot use a dangling link")
    expect_error(prepareDirectoryUpload(dest, cache=cache), "cannot use a dangling link")

    write(file=random, "YAY") # make it non-dangling, and check that it is sucked in properly.
    prepped <- prepareDirectoryUpload(dest, cache=cache)
    expect_identical(prepped$files, "arkansas")
    expect_identical(prepped$links$from.path, c("blah.txt", "foo/bar.txt"))
})

test_that("prepareDirectoryUpload handles dangling links correctly", {
    skip_on_os("windows") # giving up on flaky support for symbolic links on Windows.

    cache <- tempfile()
    dest <- tempfile()
    cloneVersion("test-R", "basic", "v1", destination=dest, cache=cache, download=FALSE)

    prepped <- prepareDirectoryUpload(dest, cache=cache)
    expect_identical(prepped$files, character(0))
    expect_identical(nrow(prepped$links), 2L)

    expect_error(prepareDirectoryUpload(dest, cache=cache, links="never"), "cannot use a dangling link")
})
