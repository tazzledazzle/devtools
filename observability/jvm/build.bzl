load("@rules_kotlin//kotlin:jvm.bzl", "kt_jvm_library")


# Every JVM service is expected to transitively depend on this. Wraps the OTel SDK + 
# your org's standard resource attributes / exporter config so individual teams never hand-roll
# their own OTel wiring.
kt_jvm_library(
	name = "otel_bootstrap",
	srcs = ["OtelBootstrap.kt"],
	visibility = ["//visibility:public"],
	deps = [
		"@maven//:io_opentelemetry_opentelemetry_api",
		"@maven//:io_opentelemetry_opentelemetry_sdk",
		"@maven//:io_opentelemetry_opentelemetry_exporter_otlp",
	],
)