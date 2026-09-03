load("@aspect_rules_ts//ts:defs.bzl", "ts_project")

# TS/Node equivalewnt of //tools/observability/jvm:otel_bootstrap
# Every Node service should transitively depend on this.
ts_project(
	name = "otel_bootstrap",
	srcs = ["otel-bootstrap.ts"],
	visibility = ["//visibility:public"],
	deps = [
		"//:node_modules/@opentelemetry/api",
		"//:node_modules/@opentelemetry/sdk-node",
		"//:node_modules/@opentelemetry/exporter-trace-otlp-grpc",
		"//:node_modules/@opentelemetry/resources",
		"//:node_modules/@opentelemetry/semantic-conventions",
	],
)