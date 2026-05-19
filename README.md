# Dynamic Load Balancing — Project 9

**Team Members**
| Name | Student ID |
|---|---|
| Sacara Samuel-Carlos | 31091001011ENSM251039 |
| Cucuteanu Lucian-Andrei | 31091001011ENSM251015 |
| Dragos Gabriel-Catalin | 31091001011ENSM251016 |

---

A minimal, self-contained testbed that demonstrates and compares three
load-balancing strategies (Round-Robin, Least-Connections, and Least-Response-Time)
in a simulated distributed system. All components run in Docker containers.

---

## Architecture

```
Your Machine
│
│  python load_test.py        (runs locally)
│       │
│       ▼ HTTP
│  ┌─────────────────────────────────────────┐
│  │          Docker Network                 │
│  │                                         │
│  │   balancer (port 9000)                  │
│  │       │                                 │
│  │       ├──► worker-1 (port 8001)         │
│  │       ├──► worker-2 (port 8002)  slow   │
│  │       └──► worker-3 (port 8003)         │
│  └─────────────────────────────────────────┘
```

### Components

| File | Role |
|---|---|
| `worker.py` | FastAPI worker — simulates processing with a configurable random delay |
| `balancer.py` | FastAPI load balancer — exposes one endpoint per algorithm |
| `load_test.py` | Async load generator — measures latency, throughput, and distribution |
| `docker-compose.yml` | Spins up all 4 containers with one command |
| `docker/Dockerfile.worker` | Container image for worker instances |
| `docker/Dockerfile.balancer` | Container image for the balancer |

---

## How It Works

### Worker (`worker.py`)
Each worker is an identical HTTP server. When it receives `GET /process` it:
1. Sleeps asynchronously for a random duration between `MIN_DELAY` and `MAX_DELAY` seconds.
2. Returns a JSON response with its own ID and how long it slept.

```json
{ "worker_id": "worker-1", "processing_time": 0.23 }
```

Worker speed is controlled entirely via environment variables in `docker-compose.yml` —
no code changes needed to switch between uniform and skewed scenarios.

### Load Balancer (`balancer.py`)

The balancer exposes **one endpoint per algorithm** — all three are live simultaneously:

| Endpoint | Algorithm | Type |
|---|---|---|
| `GET /rr/process` | Round-Robin | Baseline (stateless) |
| `GET /lc/process` | Least-Connections | Dynamic |
| `GET /lrt/process` | Least-Response-Time | Adaptive |

#### Round-Robin (`/rr/process`)
Cycles through the worker list in order: Worker 1 → 2 → 3 → 1 → …  
Stateless — does not track how busy or how fast workers are.

#### Least-Connections (`/lc/process`)
Always routes to the worker with the **fewest active (in-flight) requests**.  
When workers have different speeds, traffic naturally shifts away from slow ones.

#### Least-Response-Time (`/lrt/process`)
Routes to the worker with the **lowest average response time**, tracked using an
Exponential Moving Average (EMA) updated after every request. Failed requests
penalise the worker with their full elapsed time, so dead or slow workers are
naturally deprioritised without any explicit health checking.

The balancer reads its worker list from the `WORKERS` environment variable,
so it works both locally and inside Docker without any code changes.

### Load Generator (`load_test.py`)
Sends N requests concurrently using `asyncio + aiohttp`, prints a full report,
and **automatically saves metrics** to `metrics.json` after every run.

```
  Strategy      : lc
  Requests      : 500
  Throughput    : 42.3 req/s
  Avg latency   : 198.4 ms
  P50 latency   : 187.2 ms
  P95 latency   : 312.5 ms
  P99 latency   : 489.1 ms
  Worker distribution:
    worker-1        :  221  ########
    worker-2        :   38  #
    worker-3        :  241  ########
```

---

## Setup

### Requirements
- Python 3.10+
- Docker Desktop (download at https://www.docker.com/products/docker-desktop/)

### Install Python dependencies (for the load tester only)
```powershell
pip install -r requirements_pip.txt
```

---

## How to Run

### Option A — Docker (recommended)

#### Step 1 — Start all containers
```powershell
docker compose up --build
```

This starts 4 containers: `worker-1`, `worker-2`, `worker-3`, and `balancer`.  
Wait until you see all services print their startup messages, then proceed.

#### Step 2 — Run the load test
Open a new terminal and run:

```powershell
# Round-Robin experiment
python load_test.py --requests 1000 --url http://localhost:9000/rr/process --plot

# Least-Connections experiment
python load_test.py --requests 1000 --url http://localhost:9000/lc/process --plot

# Least-Response-Time experiment
python load_test.py --requests 1000 --url http://localhost:9000/lrt/process --plot
```

Metrics are automatically saved to `metrics.json` after each run.

#### Step 3 — Stop everything
```powershell
docker compose down
```

---

### Option B — Manual (no Docker required)

Open **5 separate terminals** and run one command in each.

**Terminal 1 — Worker 1 (fast)**
```powershell
$env:WORKER_ID="worker-1"; $env:MIN_DELAY="0.1"; $env:MAX_DELAY="0.3"; uvicorn worker:app --port 8001
```

**Terminal 2 — Worker 2 (slow)**
```powershell
$env:WORKER_ID="worker-2"; $env:MIN_DELAY="0.5"; $env:MAX_DELAY="1.0"; uvicorn worker:app --port 8002
```

**Terminal 3 — Worker 3 (fast)**
```powershell
$env:WORKER_ID="worker-3"; $env:MIN_DELAY="0.1"; $env:MAX_DELAY="0.3"; uvicorn worker:app --port 8003
```

**Terminal 4 — Balancer**
```powershell
uvicorn balancer:app --port 9000
```

**Terminal 5 — Load test**
```powershell
python load_test.py --requests 1000 --url http://localhost:9000/rr/process --plot
python load_test.py --requests 1000 --url http://localhost:9000/lc/process --plot
python load_test.py --requests 1000 --url http://localhost:9000/lrt/process --plot
```

To change worker speeds, set different `MIN_DELAY`/`MAX_DELAY` values before starting each worker terminal.  
To simulate a worker failure, just close one of the worker terminals while the test is running.

---

## Experiments

### Experiment 1 & 2 — Uniform workers (RR vs LC)

All workers run at the same speed. Edit `docker-compose.yml`, set worker-2 to fast:
```yaml
worker-2:
  environment:
    - MIN_DELAY=0.1
    - MAX_DELAY=0.3
```
Then restart and run all three endpoints:
```powershell
docker compose up --build
python load_test.py --requests 1000 --url http://localhost:9000/rr/process
python load_test.py --requests 1000 --url http://localhost:9000/lc/process
python load_test.py --requests 1000 --url http://localhost:9000/lrt/process
```
**Expected:** RR, LC, and LRT perform similarly — no algorithm has an advantage when workers are equal.

---

### Experiment 3, 4 & 5 — Heterogeneous workers / skewed (RR vs LC vs LRT) ⭐ Core experiment

Worker-2 is slow. Revert `docker-compose.yml` to:
```yaml
worker-2:
  environment:
    - MIN_DELAY=0.5
    - MAX_DELAY=1.0
```
Then restart and run all three endpoints:
```powershell
docker compose up --build
python load_test.py --requests 1000 --url http://localhost:9000/rr/process
python load_test.py --requests 1000 --url http://localhost:9000/lc/process
python load_test.py --requests 1000 --url http://localhost:9000/lrt/process
```
**Expected:**
- **RR** — latency spikes, worker-2 gets ≈33% of traffic despite being slow
- **LC** — avoids worker-2 once it accumulates active connections, lower latency than RR
- **LRT** — best performance: directly measures worker-2 is slow and routes away from it fastest

---

### Experiment 6, 7 & 8 — Worker failure (RR vs LC vs LRT)

While the load test is running, kill worker-2:
```powershell
docker compose stop worker-2
```
Run all three algorithms:
```powershell
python load_test.py --requests 1000 --url http://localhost:9000/rr/process
python load_test.py --requests 1000 --url http://localhost:9000/lc/process
python load_test.py --requests 1000 --url http://localhost:9000/lrt/process
```
**Expected:**
- **RR** — every 3rd request hits the dead worker and waits for TCP timeout (~10s), severely degrading all latency
- **LC** — gravitates toward the dead worker (counter resets to 0 after each failure), high failure rate but fast 502s
- **LRT** — penalises the dead worker's EMA with each timeout, stops routing to it progressively

Restore worker-2:
```powershell
docker compose start worker-2
```

---

## Useful Endpoints

| Endpoint | Description |
|---|---|
| `GET http://localhost:9000/rr/process` | Send a request via Round-Robin |
| `GET http://localhost:9000/lc/process` | Send a request via Least-Connections |
| `GET http://localhost:9000/lrt/process` | Send a request via Least-Response-Time |
| `GET http://localhost:9000/stats` | Per-algorithm request counts and active connections |
| `GET http://localhost:9000/health` | Balancer health + worker list + LRT averages |
| `GET http://localhost:8001/health` | Worker-1 health check |
| `GET http://localhost:8002/health` | Worker-2 health check |
| `GET http://localhost:8003/health` | Worker-3 health check |

---

## Metrics Collected

| Metric | How |
|---|---|
| **Throughput** (req/s) | total requests ÷ total wall-clock time |
| **Average latency** | mean of per-request round-trip time |
| **P50 / P95 / P99 latency** | percentiles of the latency distribution |
| **Distribution fairness** | request count per worker (from `/stats`) |
| **Active connections** | live in-flight count maintained by LC balancer |
| **Avg response time** | EMA per worker maintained by LRT balancer (from `/health`) |

All metrics are persisted to `metrics.json` automatically after every run.

---

## Project Structure

```
pcd/
├── docker/
│   ├── Dockerfile.worker      # Container image for workers
│   └── Dockerfile.balancer    # Container image for the balancer
├── balancer.py                # Load balancer (RR + LC endpoints)
├── worker.py                  # Worker HTTP service
├── load_test.py               # Async load generator + reporter
├── docker-compose.yml         # Runs all 4 containers
├── requirements_pip.txt       # Python dependencies (for load tester)
└── README.md                  # This file
```
