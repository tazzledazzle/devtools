"""rust_service macro — parallels kt_service/ts_service/go_service."""

load("@rules_rust//rust:defs.bzl", "rust_binary")

def rust_service(name, deps = [], **kwargs):
    rust_binary(
        name = name,
        deps = deps + ["//tools/observability/rust:otel_bootstrap"],
        **kwargs
    )