"""
Aspect that reports whether a build target transitively depends on an OpenTelemetry bootstrap target.

Doesn't verify that spans/metrics are used correctly, verifies the mechanism which makes correct usage possible is present. 

High signal-to-effort and doubles as free scorecard data (see scripts/aggregate_otel_coverage.py).


Usage:
	bazel build //... \
		--aspects=//tools/observability:otel_coverage.bzl%otel_coverage_aspect \
		--output_groups=otel_report

	this produces one <target>.otel_coverage.json file per target in graph,
	mirrored into bazel-bin under each target's package. Feed tree to scripts/aggregate_otel_coverage.py to get rollup.
"""

# add one label per ecosystem bootstrap target. Target counts as "instrumented" if it
# transitively depends on ANY of these
_BOOTSTRAP_LABELS = [
	"//tools/observability/jvm:otel_bootstrap",
	"//tools/observability/ts:otel_bootstrap",
]


# Rule kinds we consider "attributable" - i.e. worth reporting on as standalone 
# service/binary rather than an internal library fragment. Extend this as the build
# accumulates more binary-ish rule kinds (container_image, oci_image, etc).
_REPORTABLE_KINDS = [
	"kt_jvm_binary",
	"java_binary",
	"kt_jvm_library",
	"java_library",
	"ts_project",
	"js_binary",
	"js_library",
	"nodejs_binary",
	"go_binary",
	"go_library",
	"rust_binary",
	"rust_library",
	"py_binary",
	"py_library",
	"cc_binary",
	"cc_library",

]

OtelCoverageInfo = provider(
	doc = "Whether a target is (transitively) OTel-instrumented.",
	fields = {
		"instrumented": "bool - this target or something it depends on is the bootstrap",
		"report_file": "File - this target's own coverage report (for aggregation)",
	},
)


def _is_bootstrap(label):
	return str(label) in _BOOTSTRAP_LABELS


def _otel_coverage_aspect_impl(target, ctx):
	rule_kind = ctx.rule.rule_kind

	# walk dep-shaped attribute present on this rule. Aspects only propagate along
	# attrs listed in attr_aspects below, but we still need to read them here to decide THIS
	# target's own status.
	dep_attrs = [
		a
		for a in ("deps", "exports", "runtime_deps")
		if hasattr(ctx.rule.attr, a)
	]

	instrumented = _is_bootstrap(target.label)
	transitively_reports = []

	for attr_name in dep_attrs:
		for dep in getattr(ctx.rule.attr, attr_name):
			if OtelCoverageInfo in dep:
				info = dep[OtelCoverageInfo]
				instrumented = instrumented or info.instrumented
				transitively_reports.append(info.report_file)

	report = ctx.actions.declare_file(
		target.label.name + ".otel_coverage.json",
	)
	ctx.actions.write(
		output = report,
		content = json.encode({
			"target": str(target.label),
			"package": target.label.package,
			"kind": rule_kind,
			"reportable": rule_kind in _REPORTABLE_KINDS,
			"instrumented": instrumented,
		}),
	)

	return [
		OtelCoverageInfo(instrumented = instrumented, report_file = report),
		OutputGroupInfo(
			otel_report = depset(
				[report],
				transitive = [deptset(transitively_reports)],
			),
		),
	]

otel_coverage_aspect = aspect(
	implementation = _otel_coverage_aspect_impl,
	attr_aspects = ["deps", "exports", "runtime_deps"],
	doc = "Reports transitive dependency on the org's OTel bootstrap target(s).",
)