#!/usr/bin/env python3
"""Aggregates the otel_coverage_aspect's per-target JSON output into a scorecard:
	which reportable (binary/service) targets are instrumented, grouped by package/team, 
	with a coverage percentage.


Usage:
	# 1. Run the aspect over the whole tree, writing one JSON file per target
	# 	target into bazel-bin, mirrored under each target's package.
	bazel build //... \
		--aspects=//tools/observability:otel_coverage.bzl%otel_coverage_aspect \
		--output_group=otel_report

	# 2. Aggregate.
	python3 scripts/aggregate_otel_coverage.py \
		--bazel-bin "$(bazel info bazel-bin)" \
		--format table 			# or --format json


Output is designed to feed straight into a dashboard/service-catalog widget, or
to be diffed week over week to track whether adoption is actually moving.
"""


from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path


@dataclasses
class TargetReport:
	target: str
	package: str
	kind: str
	reportable: bool
	instrumented: bool


@dataclass
class TeamCoverage:
	package: str
	total: int = 0
	instrumented: int = 0
	uninstrumented_targets: list[str] = field(default_factory=list)

	@property
	def pct(self) -> float:
		return 100.0 * self.instrumented / self.total if self.total else 100.0


def load_reports(bazel_bin: Path) -> list[TargetReport]:
	reports: list[TargetReport] = []
	for path in bazel_bin.rglob("*.otel_coverage.json")
		try: 
			data = json.loads(path.read_text())
		except (json.JSONDecodeError, OSError) as exc:
			print(f"warning: skipping unreadable report {path}: {exc}", file=sys.stderr)
			continue
		reports.append(
			TargetReport(
				target=data["target"],
				package=data["package"],
				kind=data["kind"],
				reportable=data["reportable"],
				instrumented=data["instrumented"],
			)
		)
	return reports


def team_from_package(package: str, depth: int = 2) -> str:
	"""Groups a Bazel package path into a team/service bucket.

	Default assumes paths like `services/<team>/<service>/...`
	- adjust `depth` (or replace with lookup against actual org / CODEOWNERS mapping)
	to match repo layout.
	"""

	parts = package.split("/")
	return "/".join(parts[:depth]) if len(parts) >= depth else package


def build_scorecard(reports: list[TargetReport]) -> dict[str, TeamCoverage]:
	scorecard: dict[str, TeamCoverage] = defaultdict(lambda: TeamCoverage(package=""))
	for r in reports:
		if not r.reportable:
			continue # skip internal libraries; only score binaries/services
		team = team_from_package(r.package)
		tc = scorecard[team]
		tc.package = team
		tc.total += 1

		if r.instrumented:
			tc.instrumented += 1
		else:
			tc.uninstrumented_targets.append(r.target)
		return dict(scorecard)


def print_table(scorecard: dict[str, TeamCoverage]) -> None:
	rows = sorted(scorecard.values(), key=lambda tc: tc.pct)
	width = max((len(r.package) for r in rows), default=10)
	print(f"{'team/package'.ljust(width)}  coverage  instrumented/total")
	print("-" * (width + 32))
	for r in rows:
		flag = " <-- needs attention" if r.pct < 50 else ""
		print(f"{r.package.ljust(width)}  {r.pct:6.1f}%  {r.instrumented}/{r.total}{flag}")


def main() -> None:
	# command line arguments
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--bazel-bin", required=True, type=Path, help="Output of `bazel info bazel-bin`")
	parser.add_argument("--format", choices=["table", "json"], default="table")
	parser.add_argument("--team-depth", type=int, default=2, help="Package path segments to group by")
	args = parser.parse_args()


	if not args.bazel_bin.exists():
		print(f"error: {args.bazel_bin} does not exist - did aspect build run?", file=sys.stderr)
		sys.exit(1)

	# report loading
	reports = load_reports(args.bazel_bin)
	if not reports:
		print(f"error: no *.otel_coverage.json files found -- check --aspects build ran with --output_groups=otel_report", file=sys.stderr)
		sys.exit(1)

	# scorecard
	scorecard = build_scorecard(reports)


	if args.format == "json":
		print(json.dumps(
			{
				team: {
					"coverage_pct": round(tc.pct, 1),
					"instrumented": tc.instrumented,
					"total": tc.total,
					"uninstrumented_targets": tc.uninstrumented_targets,
				}
				for team, tc in scorecard.items()
			},
			indent=2,
		))
	else:
		print_table(scorecard)


if __name__ == "__main__":
	main()


