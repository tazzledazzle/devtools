load("@io_bazel_rules_go//go:def.bzl", "go_library")

# Go equivalent of the JVM/TS bootstrap. Gazelle keeps deps below in sync with 
# go.mod/go.sum automaticvally once this import appears in boostrap.go
# Shouldn't require hand-maintainence beyond initial `gazelle update-repos`.
go_library(
	name = "otel_bootstrap",
	srcs = ["bootstrap.go"],
	importpath = "yourorg.com/tools/observability/go",
	visibility = ["//visibility:public"],
	deps = [
		"@io_opentelemetry_go_otel//:otel",
		"@io_opentelemetry_go_otel_sdk//trace",
		"@io_opentelemetry_go_otel_sdk//resource",
		"@io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc//:otlptracegrpc",
	],
)