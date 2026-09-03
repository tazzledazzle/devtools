"""go_service macro — parallels kt_service and ts_service. Wraps
go_binary so new services inherit the OTel bootstrap dep by default."""

load("@io_bazel_rules_go//go:def.bzl", "go_binary")

def go_service(name, deps = [], **kwargs):
    go_binary(
        name = name,
        deps = deps + ["//tools/observability/go:otel_bootstrap"],
        **kwargs
    )