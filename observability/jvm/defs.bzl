load("@rules_kotlin//kotlin:jvm.bzl", "kt_jvm_binary")


def kt_service(name, deps = [], **kwargs):
	"""Defines a JVM service binary with OTel instrumentation baked in.

	Args:
		name: target name, same as kt_jvm_binary.
		deps: additional deps beyond the OTel bootstrap.
		**kwargs: forwarded to kt_jvm_binary (srcs, main_class, etc).
	"""
	kt_jvm_binary(
		name = name, 
		deps = deps + ["//tools/observability/jvm:otel_bootstrap"],
		**kwargs
	)