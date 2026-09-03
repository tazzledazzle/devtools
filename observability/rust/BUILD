load("@rules_rust//rust:defs.bzl", "rust_library")

# Rust equivalent of the other bootstraps. Crate versions come from
# your crates_universe lockfile (Cargo.Bazel.lock) — run
# `CARGO_BAZEL_REPIN=1 bazel sync --only=crate_index` after adding
# these to your workspace Cargo.toml equivalent / crate spec.
rust_library(
    name = "otel_bootstrap",
    srcs = ["bootstrap.rs"],
    visibility = ["//visibility:public"],
    deps = [
        "@crates//:opentelemetry",
        "@crates//:opentelemetry_sdk",
        "@crates//:opentelemetry-otlp",
        "@crates//:tokio",
    ],
)