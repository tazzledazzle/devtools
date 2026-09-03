load("@rules_cc//cc:defs.bzl", "cc_library")

# IMPORTANT — this is the one bootstrap in the whole set that needs
# `alwayslink = 1`. C/C++ linkers drop object files with no referenced
# symbols; if OtelBootstrap::init() is never called from a translation
# unit the linker actually walks, the .o gets dead-stripped even though
# the Bazel *dependency graph* still shows the target as a dep. That
# means the otel_coverage_aspect can report "instrumented: true" for a
# binary that has ZERO OTel code in it at runtime — a false positive
# the JVM/TS/Go/Rust equivalents don't have.
#
# alwayslink=1 forces every object file in this library into the link
# unconditionally, closing that gap. It doesn't fix "nobody actually
# calls init()" (same caveat as every other language) but it does fix
# "the linker silently threw the whole thing away".
cc_library(
    name = "otel_bootstrap",
    srcs = ["otel_bootstrap.cc"],
    hdrs = ["otel_bootstrap.h"],
    alwayslink = 1,
    visibility = ["//visibility:public"],
    deps = [
        "@opentelemetry-cpp//sdk/src/trace",
        "@opentelemetry-cpp//exporters/otlp:otlp_grpc_exporter",
    ],
)