"""
load_test.py — Async load generator for the load-balancer testbed.

Usage examples
--------------
# 200 requests, default URL
python load_test.py

# 500 requests against a specific URL
python load_test.py --requests 500 --url http://localhost:9000/process

# Save results to a JSON file and draw a latency chart
python load_test.py --requests 200 --save results.json --plot
"""

import argparse
import asyncio
import json
import statistics
import time

import aiohttp


# ---------------------------------------------------------------------------
# Core request logic
# ---------------------------------------------------------------------------
_request_counter = 0
_counter_lock = asyncio.Lock()


async def hit(session: aiohttp.ClientSession, url: str) -> dict:
    """Send one GET request and return timing + response data."""
    global _request_counter
    async with _counter_lock:
        _request_counter += 1
        n = _request_counter
    print(f"  → Sending request #{n}", flush=True)
    t0 = time.perf_counter()
    try:
        async with session.get(url) as resp:
            data = await resp.json()
            elapsed = time.perf_counter() - t0
            if resp.status != 200:
                # Balancer returned an error (e.g. 502 worker timeout)
                return {"ok": False, "latency": elapsed, "error": f"HTTP {resp.status}: {data.get('detail', '')}"}
            return {"ok": True, "latency": elapsed, **data}
    except Exception as exc:  # noqa: BLE001
        elapsed = time.perf_counter() - t0
        return {"ok": False, "latency": elapsed, "error": str(exc)}


async def run_load(n: int, url: str, concurrency: int = 50) -> list[dict]:
    """
    Send *n* requests with a sliding window of *concurrency* parallel requests.
    Using a semaphore avoids opening thousands of connections at once.
    """
    sem = asyncio.Semaphore(concurrency)

    async def bounded_hit(session):
        async with sem:
            return await hit(session, url)

    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bounded_hit(session) for _ in range(n)]
        return await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------
def compute_metrics(results: list[dict], total_time: float, n: int) -> dict:
    """Compute all summary metrics and return them as a dictionary."""
    latencies = [r["latency"] for r in results]
    ok = sum(1 for r in results if r.get("ok"))

    latencies_sorted = sorted(latencies)
    p50 = statistics.median(latencies)
    p95 = latencies_sorted[int(0.95 * len(latencies_sorted))]
    p99 = latencies_sorted[int(0.99 * len(latencies_sorted))]

    dist: dict[str, int] = {}
    for r in results:
        if r.get("ok") and "worker_id" in r:
            dist[r["worker_id"]] = dist.get(r["worker_id"], 0) + 1

    # Grab strategy from the first successful response
    strategy = next((r.get("strategy") for r in results if r.get("ok")), "unknown")

    return {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "strategy": strategy,
        "requests_total": n,
        "requests_ok": ok,
        "requests_failed": n - ok,
        "total_time_s": round(total_time, 4),
        "throughput_req_s": round(n / total_time, 2),
        "latency_ms": {
            "avg": round(statistics.mean(latencies) * 1000, 2),
            "min": round(min(latencies) * 1000, 2),
            "max": round(max(latencies) * 1000, 2),
            "p50": round(p50 * 1000, 2),
            "p95": round(p95 * 1000, 2),
            "p99": round(p99 * 1000, 2),
        },
        "worker_distribution": dist,
    }


def print_report(metrics: dict) -> None:
    d = metrics["latency_ms"]
    dist = metrics["worker_distribution"]
    n = metrics["requests_total"]

    print("\n" + "=" * 50)
    print(f"  Strategy      : {metrics['strategy']}")
    print(f"  Timestamp     : {metrics['timestamp']}")
    print(f"  Requests      : {n}")
    print(f"  Successful    : {metrics['requests_ok']}")
    print(f"  Failed        : {metrics['requests_failed']}")
    print(f"  Total time    : {metrics['total_time_s']} s")
    print(f"  Throughput    : {metrics['throughput_req_s']} req/s")
    print(f"  Avg latency   : {d['avg']} ms")
    print(f"  P50 latency   : {d['p50']} ms")
    print(f"  P95 latency   : {d['p95']} ms")
    print(f"  P99 latency   : {d['p99']} ms")
    print(f"  Min latency   : {d['min']} ms")
    print(f"  Max latency   : {d['max']} ms")
    print("-" * 50)
    print("  Worker distribution:")
    for wid, count in sorted(dist.items()):
        bar = "#" * (count // max(1, n // 40))
        print(f"    {wid:15s}: {count:4d}  {bar}")
    print("=" * 50 + "\n")


def save_results(results: list[dict], path: str) -> None:
    with open(path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Raw results saved to {path}")


def save_metrics(metrics: dict, path: str = "metrics.json") -> None:
    """
    Append this run's metrics to a JSON file so every experiment is recorded.
    The file grows into a list — one entry per run — ready to use in your paper.
    """
    existing: list[dict] = []
    try:
        with open(path) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    existing.append(metrics)

    with open(path, "w") as f:
        json.dump(existing, f, indent=2)
    print(f"Metrics appended to {path}  ({len(existing)} run(s) recorded)")


def plot_latencies(results: list[dict]) -> None:
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except ImportError:
        print("matplotlib not installed — skipping plot (pip install matplotlib)")
        return

    latencies_ms = [r["latency"] * 1000 for r in results]
    plt.figure(figsize=(10, 4))
    plt.plot(latencies_ms, alpha=0.6, linewidth=0.8, label="Latency (ms)")
    plt.axhline(y=statistics.mean(latencies_ms), color="red", linestyle="--", label="Mean")
    plt.xlabel("Request #")
    plt.ylabel("Latency (ms)")
    plt.title("Per-request latency")
    plt.legend()
    plt.tight_layout()
    plt.savefig("latency_plot.png", dpi=150)
    print("Plot saved to latency_plot.png")
    plt.show()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Load generator for the balancer testbed")
    parser.add_argument("--url", default="http://localhost:9000/lc/process")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--save", default="", help="Save raw results to JSON file")
    parser.add_argument("--plot", action="store_true", help="Draw latency chart")
    args = parser.parse_args()

    print(f"Sending {args.requests} requests to {args.url}  (concurrency={args.concurrency})")
    t0 = time.perf_counter()
    results = asyncio.run(run_load(args.requests, args.url, args.concurrency))
    total = time.perf_counter() - t0

    metrics = compute_metrics(results, total, args.requests)
    print_report(metrics)
    save_metrics(metrics)   # always saved automatically to metrics.json

    if args.save:
        save_results(results, args.save)

    if args.plot:
        plot_latencies(results)


if __name__ == "__main__":
    main()
