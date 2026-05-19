import os
import random
import asyncio

from fastapi import FastAPI

app = FastAPI()

WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")
MIN_DELAY = float(os.environ.get("MIN_DELAY", "0.1"))
MAX_DELAY = float(os.environ.get("MAX_DELAY", "0.5"))


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok", "worker": WORKER_ID}


@app.get("/process")
async def process():
    """Simulate work and return processing info."""
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    await asyncio.sleep(delay)  # non-blocking — never clogs the thread pool
    return {
        "worker_id": WORKER_ID,
        "processing_time": round(delay, 4),
    }
