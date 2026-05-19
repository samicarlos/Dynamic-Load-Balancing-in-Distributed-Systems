import os
import time
import random
import threading
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException

# ---------------------------------------------------------------------------
# Worker registry — can be overridden via environment variable
# (used when running inside Docker Compose)
# ---------------------------------------------------------------------------
_workers_env = os.environ.get(
    "WORKERS",
    "http://localhost:8001,http://localhost:8002,http://localhost:8003",
)
WORKERS: list[str] = [w.strip() for w in _workers_env.split(",")]

# ---------------------------------------------------------------------------
# Shared HTTP client (created once at startup, reused for all requests)
# ---------------------------------------------------------------------------
_http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app):
    global _http_client
    _http_client = httpx.AsyncClient(timeout=10.0)
    print(f"Balancer started. Workers: {WORKERS}")
    # Warm up LRT: send one probe request to each worker so all start
    # with a real measured average instead of 0.0 (avoids cold-start bias)
    for worker in WORKERS:
        t0 = time.perf_counter()
        try:
            await _http_client.get(f"{worker}/process")
            lrt_update(worker, time.perf_counter() - t0)
            print(f"  LRT warmup: {worker} → {_lrt_avg[worker]:.3f}s")
        except Exception:
            print(f"  LRT warmup: {worker} unreachable, skipping")
    yield
    await _http_client.aclose()


app = FastAPI(lifespan=lifespan)

# ---------------------------------------------------------------------------
# Per-strategy state
# ---------------------------------------------------------------------------

# Round-Robin
_rr_index = 0
_rr_lock = threading.Lock()

# Least-Connections
_lc_active: dict[str, int] = dict.fromkeys(WORKERS, 0)
_lc_lock = threading.Lock()

# Least-Response-Time
# Tracks an exponential moving average (EMA) of response time per worker.
# Workers with no data yet start at 0.0 and are always tried first.
_lrt_avg: dict[str, float] = dict.fromkeys(WORKERS, 0.0)
_lrt_lock = threading.Lock()
_LRT_ALPHA = 0.05  # EMA smoothing factor — lower = smoother, less sensitive to single fast responses

# Per-strategy request counters (for /stats)
rr_counts: dict[str, int] = dict.fromkeys(WORKERS, 0)
lc_counts: dict[str, int] = dict.fromkeys(WORKERS, 0)
lrt_counts: dict[str, int] = dict.fromkeys(WORKERS, 0)


# ---------------------------------------------------------------------------
# Picking strategies
# ---------------------------------------------------------------------------
def rr_pick() -> str:
    """Round-Robin: cycle through workers in order."""
    global _rr_index
    with _rr_lock:
        worker = WORKERS[_rr_index]
        _rr_index = (_rr_index + 1) % len(WORKERS)
    return worker


def lc_pick() -> str:
    """Least-Connections: route to the worker with the fewest active requests."""
    with _lc_lock:
        return min(WORKERS, key=lambda w: _lc_active[w])


def lrt_pick() -> str:
    """Least-Response-Time: route to the worker with the lowest average response time.
    Workers within 30% of the best average are treated as equivalent and chosen randomly,
    preventing a single lucky worker from monopolising all traffic."""
    with _lrt_lock:
        best = min(_lrt_avg.values())
        threshold = best * 1.30  # 3% tolerance band
        candidates = [w for w in WORKERS if _lrt_avg[w] <= threshold]
        return random.choice(candidates)


def lrt_update(worker: str, elapsed: float) -> None:
    """Update the EMA response time for a worker after a completed request."""
    with _lrt_lock:
        _lrt_avg[worker] = _LRT_ALPHA * elapsed + (1 - _LRT_ALPHA) * _lrt_avg[worker]


# ---------------------------------------------------------------------------
# Shared forwarding logic
# ---------------------------------------------------------------------------
async def forward(worker: str, strategy: str) -> dict:
    """Forward one request to *worker* and return the enriched response."""
    try:
        response = await _http_client.get(f"{worker}/process")
        response.raise_for_status()
        result = response.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    result["routed_to"] = worker
    result["strategy"] = strategy
    return result


# ---------------------------------------------------------------------------
# Routes — one endpoint per strategy
# ---------------------------------------------------------------------------
@app.get("/rr/process")
async def process_rr():
    """Round-Robin: forward to the next worker in the cycle."""
    worker = rr_pick()
    with _rr_lock:
        rr_counts[worker] = rr_counts.get(worker, 0) + 1
    return await forward(worker, "rr")


@app.get("/lc/process")
async def process_lc():
    """Least-Connections: forward to the worker with fewest active requests."""
    worker = lc_pick()

    with _lc_lock:
        _lc_active[worker] += 1
        lc_counts[worker] = lc_counts.get(worker, 0) + 1

    try:
        return await forward(worker, "lc")
    finally:
        with _lc_lock:
            _lc_active[worker] -= 1


@app.get("/lrt/process")
async def process_lrt():
    """Least-Response-Time: forward to the worker with the lowest average response time."""
    worker = lrt_pick()

    with _lrt_lock:
        lrt_counts[worker] = lrt_counts.get(worker, 0) + 1

    t0 = time.perf_counter()
    try:
        result = await forward(worker, "lrt")
        lrt_update(worker, time.perf_counter() - t0)
        return result
    except HTTPException:
        # Penalise the worker with the full elapsed time so it gets deprioritised
        lrt_update(worker, time.perf_counter() - t0)
        raise


# ---------------------------------------------------------------------------
# Utility routes
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    """Balancer health check."""
    return {
        "status": "ok",
        "workers": WORKERS,
        "lc_active_connections": dict(_lc_active),
        "lrt_avg_response_time_s": {w: round(v, 4) for w, v in _lrt_avg.items()},
    }


@app.get("/stats")
def stats():
    """Per-strategy request distribution across workers."""
    return {
        "round_robin": {
            "request_counts": dict(rr_counts),
            "active_connections": "N/A — RR is stateless, it does not track worker load",
        },
        "least_connections": {
            "request_counts": dict(lc_counts),
            "active_connections": dict(_lc_active),
        },
        "least_response_time": {
            "request_counts": dict(lrt_counts),
            "avg_response_time_s": {w: round(v, 4) for w, v in _lrt_avg.items()},
        },
    }
