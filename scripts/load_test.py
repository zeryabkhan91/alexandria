#!/usr/bin/env python3
"""Simple local load-test utility for the Alexandria web API."""

from __future__ import annotations

import argparse
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import URLError
from urllib.request import urlopen


DEFAULT_PATHS = [
    "/api/health",
    "/api/review-data?limit=25&offset=0",
    "/api/iterate-data?limit=25&offset=0",
    "/api/generation-history?limit=50&offset=0",
    "/api/similarity-matrix?threshold=0.25&limit=50&offset=0",
    "/api/mockup-status?limit=25&offset=0",
    "/api/analytics/costs",
    "/api/analytics/completion",
]


def _request(base_url: str, path: str) -> tuple[bool, float]:
    start = time.perf_counter()
    try:
        with urlopen(f"{base_url}{path}", timeout=15) as response:
            _ = response.read(64)
            ok = int(getattr(response, "status", 200)) < 500
    except URLError:
        ok = False
    latency_ms = (time.perf_counter() - start) * 1000.0
    return ok, latency_ms


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    rank = max(0, min(len(values) - 1, int(round((p / 100.0) * (len(values) - 1)))))
    return sorted(values)[rank]


def run_load_test(*, base_url: str, duration_seconds: int, users: int) -> dict[str, float]:
    deadline = time.time() + max(1, int(duration_seconds))
    latencies: list[float] = []
    errors = 0
    requests_total = 0

    with ThreadPoolExecutor(max_workers=max(1, int(users))) as pool:
        futures = []
        while time.time() < deadline:
            for _ in range(max(1, int(users))):
                path = random.choice(DEFAULT_PATHS)
                futures.append(pool.submit(_request, base_url, path))
            done = []
            for future in as_completed(futures, timeout=30):
                done.append(future)
                ok, latency = future.result()
                requests_total += 1
                latencies.append(latency)
                if not ok:
                    errors += 1
                if len(done) >= users:
                    break
            futures = [f for f in futures if f not in done]

    elapsed = max(1e-6, float(duration_seconds))
    throughput = requests_total / elapsed
    return {
        "requests_total": float(requests_total),
        "errors": float(errors),
        "error_rate": (errors / requests_total) if requests_total else 0.0,
        "p50_ms": _percentile(latencies, 50),
        "p95_ms": _percentile(latencies, 95),
        "p99_ms": _percentile(latencies, 99),
        "avg_ms": statistics.fmean(latencies) if latencies else 0.0,
        "throughput_rps": throughput,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a simple API load test")
    parser.add_argument("--base-url", type=str, default="http://127.0.0.1:8001")
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--users", type=int, default=10)
    args = parser.parse_args()
    report = run_load_test(base_url=args.base_url.rstrip("/"), duration_seconds=args.duration, users=args.users)
    print(f"P95 response time: {report['p95_ms']:.2f}ms")
    print(f"Throughput: {report['throughput_rps']:.2f} req/s")
    print(f"Total requests: {int(report['requests_total'])}, errors: {int(report['errors'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
