"""Task queue optimized implementation.
- Uses ThreadPoolExecutor + bounded queue.Queue(maxsize=N)
- SQLite-backed idempotency (UNIQUE constraint)
- Retry with exponential backoff + jitter
- Metrics collection (enqueue/dequeue/success/fail/retry, latency mean/P95)
- Streaming I/O simulation (chunked read/write to local "storage")

Run: python task_queue_optimized.py --benchmark
"""
from __future__ import annotations
import argparse
import json
import logging
import random
import queue
import sqlite3
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

LOG = logging.getLogger("task_queue_opt")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Exceptions for classification

class TransientError(Exception):
    pass

class PermanentError(Exception):
    pass

# --- Metrics

@dataclass
class Metrics:
    enqueued: int = 0
    dequeued: int = 0
    success: int = 0
    failed: int = 0
    retries: int = 0
    rejected: int = 0
    latencies: List[float] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def as_dict(self) -> Dict[str, Any]:
        with self.lock:
            lat = list(self.latencies)
            mean = statistics.mean(lat) if lat else 0.0
            p95 = self._p95(lat)
            return {
                "enqueued": self.enqueued,
                "dequeued": self.dequeued,
                "success": self.success,
                "failed": self.failed,
                "retries": self.retries,
                "rejected": self.rejected,
                "mean_latency_ms": mean * 1000,
                "p95_latency_ms": p95 * 1000,
                "samples": len(lat),
            }

    def _p95(self, lat: List[float]) -> float:
        if not lat:
            return 0.0
        s = sorted(lat)
        idx = min(len(s) - 1, max(0, int(0.95 * len(s)) - 1))
        return s[idx]

# --- Idempotency store (SQLite)

class IdempotencyStore:
    def __init__(self, path: str = ":memory:"):
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self._init_table()

    def _init_table(self):
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed (
                    idempotency_key TEXT PRIMARY KEY,
                    status TEXT,
                    processed_at REAL
                )
                """
            )

    def is_processed(self, key: str) -> bool:
        with self._lock:
            cur = self._conn.execute("SELECT 1 FROM processed WHERE idempotency_key = ?", (key,))
            return cur.fetchone() is not None

    def mark_processed(self, key: str, status: str = "success"):
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO processed (idempotency_key, status, processed_at) VALUES (?, ?, ?)",
                    (key, status, time.time()),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                # already marked
                pass

# --- Processor (core task logic)

class Processor:
    def __init__(self, storage_dir: Optional[str] = None, error_rate: float = 0.0, mean_latency: float = 0.03):
        self.storage_dir = Path(storage_dir) if storage_dir else None
        self.error_rate = error_rate
        self.mean_latency = mean_latency

    def process(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate processing with optional transient/permanent errors and streaming I/O."""
        # Fault injection
        p = random.random()
        if p < task.get("permanent_error_chance", 0.0):
            raise PermanentError("bad task parameter")
        if p < task.get("transient_error_chance", self.error_rate):
            raise TransientError("transient failure")

        # Simulate I/O/CPU work with jitter around mean_latency
        delay = random.uniform(self.mean_latency * 0.5, self.mean_latency * 1.5)
        # streaming I/O simulation (chunked sleep)
        chunks = max(1, int(delay / 0.01))
        for _ in range(chunks):
            time.sleep(delay / chunks)

        # Optional streaming write to local storage
        if self.storage_dir:
            out = self.storage_dir / f"out-{task.get('id')}.bin"
            out.parent.mkdir(parents=True, exist_ok=True)
            with out.open("wb") as f:
                remaining = 1024
                chunk = b"x" * 256
                while remaining > 0:
                    f.write(chunk[:min(remaining, len(chunk))])
                    remaining -= len(chunk)
                    f.flush()
        return {"image_key": task.get("image_key"), "variant": task.get("variant"), "status": "ok"}

# --- TaskQueue using bounded queue + ThreadPoolExecutor

class TaskQueue:
    def __init__(self, max_workers: int = 4, queue_size: int = 100, block_on_full: bool = True, idempotency_db: str = ":memory:"):
        self.queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.metrics = Metrics()
        self._stop_event = threading.Event()
        self._submitter_thread: Optional[threading.Thread] = None
        self.idempotency = IdempotencyStore(idempotency_db)
        self.dead_letter: List[Dict[str, Any]] = []
        self.max_workers = max_workers
        self.block_on_full = block_on_full
        self.retry_limit = 3
        # track keys that are pending (queued or processing) to avoid duplicate work
        self._pending_lock = threading.Lock()
        self._pending: set[str] = set()

    def start(self):
        LOG.info("Starting submitter thread (workers=%d)", self.max_workers)
        self._submitter_thread = threading.Thread(target=self._submitter_loop, daemon=True)
        self._submitter_thread.start()

    def stop(self, wait: bool = True):
        LOG.info("Stopping TaskQueue")
        self._stop_event.set()
        if self._submitter_thread:
            self._submitter_thread.join()
        self.executor.shutdown(wait=wait)

    def enqueue(self, task: Dict[str, Any]) -> bool:
        key = self._idempotency_key(task)
        # fast-path: if already processed — skip
        if self.idempotency.is_processed(key):
            LOG.debug("Duplicate task skipped (already processed): %s", key)
            return True
        # if another identical task is queued/processing, skip enqueue to avoid duplicate work
        with self._pending_lock:
            if key in self._pending:
                LOG.debug("Duplicate task skipped (pending): %s", key)
                return True
            # reserve the key so subsequent enqueues will be no-ops
            self._pending.add(key)
        try:
            if self.block_on_full:
                self.queue.put(task, block=True, timeout=5)
                self.metrics.enqueued += 1
                return True
            else:
                self.queue.put(task, block=False)
                self.metrics.enqueued += 1
                return True
        except queue.Full:
            # release pending reservation on rejection
            with self._pending_lock:
                self._pending.discard(key)
            LOG.warning("Queue full, rejecting task %s", task.get("id"))
            self.metrics.rejected += 1
            return False

    def _idempotency_key(self, task: Dict[str, Any]) -> str:
        return f"{task.get('image_key')}::{task.get('variant')}"

    def _submitter_loop(self):
        while not self._stop_event.is_set() or not self.queue.empty():
            try:
                task = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            self.metrics.dequeued += 1
            # submit to threadpool for processing with retry wrapper
            self.executor.submit(self._process_with_retries, task)
            self.queue.task_done()

    def _process_with_retries(self, task: Dict[str, Any]):
        key = self._idempotency_key(task)
        if self.idempotency.is_processed(key):
            LOG.debug("Already processed at process-time: %s", key)
            with self._pending_lock:
                self._pending.discard(key)
            return
        attempt = 0
        start = time.perf_counter()
        try:
            while attempt <= self.retry_limit:
                try:
                    attempt += 1
                    res = Processor(storage_dir=None).process(task)
                    latency = time.perf_counter() - start
                    with self.metrics.lock:
                        self.metrics.latencies.append(latency)
                        self.metrics.success += 1
                    self.idempotency.mark_processed(key, status="success")
                    LOG.info("Processed %s (attempt=%d, latency=%.3f)s", key, attempt, latency)
                    return res
                except TransientError as e:
                    self.metrics.retries += 1
                    LOG.warning("Transient error for %s (attempt=%d): %s", key, attempt, e)
                    if attempt > self.retry_limit:
                        break
                    backoff = (2 ** (attempt - 1)) * 0.01
                    backoff = backoff * (1 + random.uniform(-0.3, 0.3))
                    time.sleep(backoff)
                    continue
                except PermanentError as e:
                    LOG.error("Permanent error for %s: %s", key, e)
                    break
                except Exception as e:
                    LOG.exception("Unhandled error processing %s: %s", key, e)
                    break
            # If we reach here, task failed
            with self.metrics.lock:
                self.metrics.failed += 1
            self.dead_letter.append(task)
            self.idempotency.mark_processed(key, status="failed")
            LOG.error("Task moved to dead-letter: %s", key)
        finally:
            # always remove from pending set so future tasks with same key can be processed
            with self._pending_lock:
                self._pending.discard(key)

# --- Benchmark & CLI

def load_tasks(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    with p.open('r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def expand_tasks(tasks: List[Dict[str, Any]], total: int) -> List[Dict[str, Any]]:
    out = []
    i = 0
    while len(out) < total:
        t = dict(tasks[i % len(tasks)])
        t['id'] = len(out) + 1
        out.append(t)
        i += 1
    return out

def run_benchmark(task_file: str, total_tasks: int = 200, workers: int = 8, queue_size: int = 200, block_on_full: bool = True) -> Dict[str, Any]:
    tasks = load_tasks(task_file)
    tasks = expand_tasks(tasks, total_tasks)
    tq = TaskQueue(max_workers=workers, queue_size=queue_size, block_on_full=block_on_full, idempotency_db=":memory:")
    tq.start()

    start = time.perf_counter()
    # producers: push all tasks
    for t in tasks:
        ok = tq.enqueue(t)
        if not ok:
            LOG.warning("Task rejected during benchmark: %s", t.get('id'))
    # wait for queue to drain
    tq.queue.join()
    tq.stop(wait=True)
    duration = time.perf_counter() - start
    metrics = tq.metrics.as_dict()
    metrics.update({
        'duration_s': duration,
        'throughput_qps': metrics['success'] / duration if duration > 0 else 0.0,
        'dead_letter': len(tq.dead_letter),
        'queue_size': queue_size,
        'workers': workers,
        'total_tasks': total_tasks,
    })
    # write metrics JSON for inspection
    out_path = Path('metrics_optimized.json')
    out_path.write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics))
    return metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--benchmark', action='store_true')
    parser.add_argument('--tasks-file', default=str(Path('input_new') / 'sample_tasks_small.json'))
    parser.add_argument('--total', type=int, default=200)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--queue-size', type=int, default=200)
    parser.add_argument('--block', action='store_true', help='Block producer when queue full')
    args = parser.parse_args()

    if args.benchmark:
        run_benchmark(task_file=args.tasks_file, total_tasks=args.total, workers=args.workers, queue_size=args.queue_size, block_on_full=args.block)

if __name__ == '__main__':
    main()
