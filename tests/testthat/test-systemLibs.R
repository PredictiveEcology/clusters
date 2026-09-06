## Shipping missing system shared libraries to hosts where you have no root.
## The cluster-bound half needs a live host; the policy that decides WHAT gets
## shipped is split out precisely so it can be tested here.

id <- function(arch = "x86_64", libc = "2.39") list(arch = arch, libc = libc)
plan <- clusters:::.planSystemLibShipment

test_that("nothing missing means nothing to do", {
  p <- plan(character(0), id(), id())
  expect_length(p$ship, 0L)
  expect_length(p$unmet, 0L)
})

test_that("an ordinary library the master can resolve is shipped", {
  p <- plan("libtbb.so.12", id(), id(), resolve = function(x) "/lib/libtbb.so.12")
  expect_equal(p$ship, "libtbb.so.12")
  expect_length(p$unmet, 0L)
})

test_that("the core runtime is never shipped", {
  ## Overriding the host's own libc/libstdc++ via LD_LIBRARY_PATH breaks far
  ## more than it fixes: everything else on the machine is linked against them.
  core <- c("libc.so.6", "libstdc++.so.6", "libgcc_s.so.1", "libm.so.6",
            "libpthread.so.0", "libdl.so.2", "librt.so.1", "ld-linux-x86-64.so.2")
  p <- plan(core, id(), id(), resolve = function(x) "/lib/whatever")
  expect_length(p$ship, 0L)
  expect_setequal(p$unmet, core)
})

test_that("core libs are excluded but their non-core neighbours still ship", {
  p <- plan(c("libc.so.6", "libtbb.so.12"), id(), id(),
            resolve = function(x) "/lib/x")
  expect_equal(p$ship, "libtbb.so.12")
  expect_equal(p$unmet, "libc.so.6")
})

test_that("a platform mismatch ships nothing at all", {
  ## Copying a compiled object to a host with a different arch or libc is not
  ## sound; report it rather than hand over something that cannot load -- or,
  ## worse, one that can.
  p <- plan("libtbb.so.12", id(arch = "aarch64"), id(), resolve = function(x) "/lib/x")
  expect_length(p$ship, 0L)
  expect_equal(p$unmet, "libtbb.so.12")

  p2 <- plan("libtbb.so.12", id(libc = "2.35"), id(), resolve = function(x) "/lib/x")
  expect_length(p2$ship, 0L)
  expect_equal(p2$unmet, "libtbb.so.12")
})

test_that("a library the master cannot resolve is reported, not silently dropped", {
  p <- plan(c("libtbb.so.12", "libnowhere.so.9"), id(), id(),
            resolve = function(x) if (identical(x, "libtbb.so.12")) "/lib/x" else NA_character_)
  expect_equal(p$ship, "libtbb.so.12")
  expect_equal(p$unmet, "libnowhere.so.9")
})

test_that(".resolveSoname finds a library this machine actually has", {
  skip_on_os(c("windows", "mac"))
  skip_if(nzchar(Sys.which("ldconfig")) == FALSE, "no ldconfig")
  p <- clusters:::.resolveSoname("libc.so.6")
  expect_true(is.na(p) || file.exists(p))
  expect_true(is.na(clusters:::.resolveSoname("libdefinitelynotreal.so.999")))
})

test_that(".platformId reports an arch and a libc", {
  skip_on_os(c("windows", "mac"))
  pid <- clusters:::.platformId()
  expect_named(pid, c("arch", "libc"))
  expect_true(nzchar(pid$arch))
})

test_that("the core-lib pattern does not catch unrelated libraries", {
  ## Regression: the pattern required a literal "." right after the name, so
  ## `ld-linux-x86-64.so.2` -- the dynamic loader itself -- slipped through and
  ## would have been shipped. Widening it to `[.-]` must not start swallowing
  ## libcurl (libc...) or libmagick++ (libm...).
  ok <- c("libcurl.so.4", "libmagick++-7.Q16HDRI.so.10", "libcrypto.so.3",
          "libmariadb.so.3", "libdeflate.so.0", "librtmp.so.1", "libtbb.so.12")
  p <- plan(ok, id(), id(), resolve = function(x) "/lib/x")
  expect_setequal(p$ship, ok)
  expect_length(p$unmet, 0L)
})
