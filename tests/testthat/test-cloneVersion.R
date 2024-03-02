# This checks the cloneVersion function.
# library(testthat); library(gypsum); source("test-cloneVersion.R")

test_that("cloneVersion works as expected with existing files", {
    cache <- tempfile()
    dest <- tempfile()
    out <- cloneVersion("test-R", "basic", "v1", destination=dest, cache=cache)

    d1 <- file.path(dest, "blah.txt")
    if (.Platform$OS.type != "windows") { # as windows might not support symbolic links.
        l1 <- Sys.readlink(d1)
        expect_true(file.exists(l1))
        expect_true(endsWith(l1, "test-R/basic/v1/blah.txt"))
    } else {
        expect_true(file.exists(d1))
    }

    d2 <- file.path(dest, "foo/bar.txt")
    if (.Platform$OS.type != "windows") {
        l2 <- Sys.readlink(d2)
        expect_true(file.exists(l2))
        expect_true(endsWith(l2, "test-R/basic/v1/foo/bar.txt"))
    } else {
        expect_true(file.exists(d2))
    }
})

test_that("cloneVersion happily works without any download", {
    skip_on_os("windows") # download=FALSE can't work on windows if symbolic links aren't available.

    cache <- tempfile()
    dest <- tempfile()
    out <- cloneVersion("test-R", "basic", "v1", download=FALSE, destination=dest, cache=cache)

    l1 <- Sys.readlink(file.path(dest, "blah.txt"))
    expect_true(endsWith(l1, "test-R/basic/v1/blah.txt"))
    expect_false(file.exists(l1))

    l2 <- Sys.readlink(file.path(dest, "foo/bar.txt"))
    expect_true(endsWith(l2, "test-R/basic/v1/foo/bar.txt"))
    expect_false(file.exists(l2))
})

test_that("cloneVersion works correctly if version itself contains links", {
    skip_on_os("windows") # download=FALSE can't work on windows if symbolic links aren't available.

    cache <- tempfile()
    dest <- tempfile()
    out <- cloneVersion("test-R", "basic", "v2", destination=dest, cache=cache, download=FALSE, relink=FALSE)

    l1 <- Sys.readlink(file.path(dest, "blah.txt"))
    expect_true(endsWith(l1, "test-R/basic/v2/blah.txt"))
    expect_false(file.exists(l1))

    l2 <- Sys.readlink(file.path(dest, "foo/bar.txt"))
    expect_true(endsWith(l2, "test-R/basic/v2/foo/bar.txt"))
    expect_false(file.exists(l2))
})
