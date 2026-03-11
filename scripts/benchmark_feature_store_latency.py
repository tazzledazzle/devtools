#!/usr/bin/env python3
"""
Benchmark feature store read latency for local-dev validation.

Seeds a configurable number of keys with small feature vectors, then performs
repeated random reads via the same lookup path used for online scoring. Reports
p50, p95, max latency and QPS, and whether p95 meets the target budget (< 5 ms).

Usage:
  REDIS_URL=redis://localhost:6379/0 python scripts/benchmark_feature_store_latency.py
  python scripts/benchmark_feature_store_latency.py --keys 100 --operations 5000

Requires Redis running (default: redis://localhost:6379/0).
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time

# Allow running from repo root with src on path
if __name__ == "__main__" and "__file__" in dir():
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    _src = os.path.join(_root, "src")
    if _src not in sys.path:
        sys.path.insert(0, _src)

from stores.stores import feature_key_user, get_default_feature_store
from stores.feature_lookup import get_features_for_id

TARGET_P95_MS = 5.0
DEFAULT_KEYS = 100
DEFAULT_OPERATIONS = 5_000
DEFAULT_REDIS_URL = "redis://localhost:6379/0"


def make_sample_features(n_fields: int = 15) -> dict[str, float]:
    """Return a small feature vector with n_fields for seeding."""
    return {f"f{i}": random.random() for i in range(n_fields)}


def run_benchmark(
    num_keys: int,
    num_operations: int,
    redis_url: str | None,
) -> tuple[list[float], float]:
    """Seed keys, run random reads, return list of latencies (seconds) and total time."""
    if redis_url:
        os.environ["REDIS_URL"] = redis_url
    store = get_default_feature_store()
    # Seed by user_id keys (features:user:0 .. features:user:num_keys-1)
    for i in range(num_keys):
        key = feature_key_user(str(i))
        store.set_features(key, make_sample_features())
    # Random read latency samples
    latencies: list[float] = []
    user_ids = [str(random.randint(0, num_keys - 1)) for _ in range(num_operations)]
    start = time.perf_counter()
    for uid in user_ids:
        t0 = time.perf_counter()
        get_features_for_id(user_id=uid, store=store)
        latencies.append((time.perf_counter() - t0) * 1000.0)  # ms
    total_s = time.perf_counter() - start
    return latencies, total_s


def percentile(sorted_values: list[float], p: float) -> float:
    """Return p-th percentile (0..100) of sorted values."""
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_values) else f
    return sorted_values[f] + (k - f) * (sorted_values[c] - sorted_values[f])


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark feature store read latency")
    parser.add_argument(
        "--keys",
        type=int,
        default=int(os.environ.get("BENCH_KEYS", DEFAULT_KEYS)),
        help=f"Number of keys to seed (default: {DEFAULT_KEYS})",
    )
    parser.add_argument(
        "--operations",
        type=int,
        default=int(os.environ.get("BENCH_OPERATIONS", DEFAULT_OPERATIONS)),
        help=f"Number of read operations (default: {DEFAULT_OPERATIONS})",
    )
    parser.add_argument(
        "--redis-url",
        type=str,
        default=os.environ.get("REDIS_URL", DEFAULT_REDIS_URL),
        help="Redis URL (default: REDIS_URL or redis://localhost:6379/0)",
    )
    args = parser.parse_args()
    if args.keys < 1 or args.operations < 1:
        print("error: --keys and --operations must be >= 1", file=sys.stderr)
        sys.exit(1)
    print(f"Seeding {args.keys} keys, then {args.operations} random reads (Redis: {args.redis_url})...")
    try:
        latencies, total_s = run_benchmark(args.keys, args.operations, args.redis_url)
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    latencies.sort()
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    max_ms = latencies[-1] if latencies else 0.0
    qps = args.operations / total_s if total_s > 0 else 0.0
    print()
    print("Latency (ms):  p50 = {:.3f}  p95 = {:.3f}  max = {:.3f}".format(p50, p95, max_ms))
    print("QPS: {:.1f}".format(qps))
    print()
    if p95 < TARGET_P95_MS:
        print("OK: p95 = {:.2f} ms < {:.0f} ms target".format(p95, TARGET_P95_MS))
    else:
        print("FAIL: p95 = {:.2f} ms >= {:.0f} ms target".format(p95, TARGET_P95_MS))


if __name__ == "__main__":
    main()
