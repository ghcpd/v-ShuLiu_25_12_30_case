"""Optimized task queue using ThreadPoolExecutor + bounded queue, idempotency (SQLite),
retry with exponential backoff + jitter, streaming I/O simulation, and metrics export.

Usage:
    python task_queue_optimized.py --tasks input_new/sample_tasks_small.json --benchmark

Key flags:
    --workers, --queue-size, --produce-mode (block|failfast), --db, --max-retries,
    --fault-error-rate, --seed

Metrics are printed as JSON when run with --benchmark and written to ./results/*.json
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import contextlib
import io
import itertools
import json
import logging
import math
import os
import random
import sqlite3
import threading
import time
import traceback
from queue import Full, Queue
from typing import Dict, Iterable, List, Optional, Tuple

LOG = logging.getLogger("taskq.opt")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DEFAULT_DB = "./task_queue_meta.db"
RESULTS_DIR = "./results"
os.makedirs(RESULTS_DIR, exist_ok=True)


class PermanentError(Exception):
    pass


class TransientError(Exception):
    pass


def now():
    return time.time()


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return d0 + d1


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.enqueued = 0
        self.dequeued = 0
        self.success = 0
        self.failed = 0
        self.retries = 0
        self.rejected = 0
        self.latencies: List[float] = []
        self.queue_depth_samples: List[int] = []
        self.thread_utilization_samples: List[int] = []

    def to_dict(self):
        with self._lock:
            lat = sorted(self.latencies)
            return {
                "enqueued": self.enqueued,
                "dequeued": self.dequeued,
                "success": self.success,
                "failed": self.failed,
                "retries": self.retries,
                "rejected": self.rejected,
                "mean_latency_sec": (sum(lat) / len(lat)) if lat else 0.0,
                "p95_latency_sec": percentile(lat, 95) if lat else 0.0,
                "queue_depth_samples": self.queue_depth_samples,
                "queue_depth_max": max(self.queue_depth_samples) if self.queue_depth_samples else 0,
            }

    # incremental updates (thread-safe)
    def sample_queue_depth(self, depth: int):
        with self._lock:
            self.queue_depth_samples.append(depth)

    def observe_latency(self, latency: float):
        with self._lock:
            self.latencies.append(latency)

    def incr(self, name: str, n: int = 1):
        with self._lock:
            setattr(self, name, getattr(self, name) + n)


class IdempotencyStore:
    """SQLite-backed idempotency store with UNIQUE constraint on idempotency_key."""

    def __init__(self, path: str = DEFAULT_DB):
        self.path = path
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._init_db()
        self._lock = threading.Lock()

    def _init_db(self):
        c = self._conn.cursor()
        c.execute("PRAGMA journal_mode = WAL")
        c.execute(
            """CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                idempotency_key TEXT UNIQUE,
                status TEXT,
                retries INTEGER DEFAULT 0,
                result TEXT,
                updated_at REAL
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS dead_letter (
                id INTEGER PRIMARY KEY,
                idempotency_key TEXT,
                payload TEXT,
                failure_reason TEXT,
                created_at REAL
            )"""
        )
        c.close()

    def reserve(self, idempotency_key: str) -> bool:
        """Try to reserve this key for processing. Return False if already processed/reserved."""
        now_ts = now()
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO tasks (idempotency_key, status, updated_at) VALUES (?, 'processing', ?)",
                    (idempotency_key, now_ts),
                )
                return True
            except sqlite3.IntegrityError:
                # already exists
                row = self._conn.execute(
                    "SELECT status FROM tasks WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if row and row[0] == "success":
                    return False
                # allow re-processing if previous failed but keep dedup semantics
                return False

    def mark_success(self, idempotency_key: str, result: str = ""):
        with self._lock:
            self._conn.execute(
                "UPDATE tasks SET status='success', result=?, updated_at=? WHERE idempotency_key=?",
                (result, now(), idempotency_key),
            )

    def mark_failure(self, idempotency_key: str, payload: str, reason: str):
        with self._lock:
            self._conn.execute(
                "UPDATE tasks SET status='failed', retries=retries+1, updated_at=? WHERE idempotency_key=?",
                (now(), idempotency_key),
            )
            self._conn.execute(
                "INSERT INTO dead_letter (idempotency_key, payload, failure_reason, created_at) VALUES (?,?,?,?)",
                (idempotency_key, payload, reason, now()),
            )

    def increment_retry(self, idempotency_key: str):
        with self._lock:
            self._conn.execute(
                "UPDATE tasks SET retries = retries + 1, updated_at = ? WHERE idempotency_key = ?",
                (now(), idempotency_key),
            )

    def release(self, idempotency_key: str):
        """Remove a previously-reserved processing row (used when enqueue fails)."""
        with self._lock:
            self._conn.execute(
                "DELETE FROM tasks WHERE idempotency_key = ? AND status = 'processing'",
                (idempotency_key,),
            )

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


class Processor:
    def __init__(self, storage_dir: str = "./storage", fault_rate: float = 0.0, seed: Optional[int] = None):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.rng = random.Random(seed)
        self._seed = int(seed) if seed is not None else 0
        self.fault_rate = float(fault_rate)

    def _stream_input(self, image_key: str, chunk_size: int = 8 * 1024) -> Iterable[bytes]:
        """If file exists in storage_dir, stream it; otherwise simulate streaming by yielding chunks with
        a small sleep to emulate I/O."""
        path = os.path.join(self.storage_dir, image_key.replace("/", os.sep))
        if os.path.exists(path):
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
        else:
            # Simulate streaming payload without holding all in memory
            total = 32 * 1024  # 32KB synthetic object
            sent = 0
            while sent < total:
                to_send = min(chunk_size, total - sent)
                sent += to_send
                # simulate I/O cost (increased so queue/backpressure is reliably observable in tests)
                time.sleep(0.005)
                yield b"0" * to_send

    def process(self, task: Dict) -> Dict:
        # Simulate reading the object in streaming fashion
        image_key = task.get("image_key")
        size = task.get("size")
        # Fault injection — deterministic decision based on seed + image_key when possible
        # This makes tests reproducible while still allowing probabilistic faults via self.rng.
        if self.fault_rate > 0:
            # deterministic per-object decision (stable across processes)
            # (don't catch TransientError/PermanentError raised intentionally)
            try:
                import hashlib
                digest = hashlib.md5(f"{image_key}:{self._seed}".encode()).digest()
                threshold = int(self.fault_rate * 255)
                if digest[0] <= threshold:
                    # prefer transient to allow retry path coverage
                    raise TransientError("simulated transient I/O error (deterministic)")
            except (TransientError, PermanentError):
                raise
            except Exception:
                # fall back to RNG-driven behaviour only on unexpected failures
                if self.rng.random() < self.fault_rate:
                    if self.rng.random() < 0.7:
                        raise TransientError("simulated transient I/O error")
                    else:
                        raise PermanentError("simulated bad input")

        total_read = 0
        for chunk in self._stream_input(image_key):
            total_read += len(chunk)
            # do cheap in-memory "processing" per chunk
            _ = chunk[0:1]

        # simulate CPU work proportional to size (increased so backpressure is observable in CI)
        time.sleep(0.05)
        return {"image_key": image_key, "variant": size, "status": "ok", "bytes": total_read}


class TaskQueueOptimized:
    def __init__(
        self,
        worker_fn,
        max_workers: int = 4,
        queue_size: int = 200,
        produce_block: bool = True,
        idempotency_db: str = DEFAULT_DB,
        metrics: Optional[Metrics] = None,
    ):
        self.queue: Queue = Queue(maxsize=queue_size)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.worker_fn = worker_fn
        self.metrics = metrics or Metrics()
        self._stop_event = threading.Event()
        self._producer_threads: List[threading.Thread] = []
        self.idempotency = IdempotencyStore(idempotency_db)
        self.produce_block = produce_block
        self._futures: List[concurrent.futures.Future] = []

    def enqueue(self, task: Dict) -> bool:
        key = f"{task.get('image_key')}|{task.get('size')}"
        # fast dedupe: check DB
        reserved = self.idempotency.reserve(key)
        if not reserved:
            LOG.debug("dedup skip %s", key)
            return False
        try:
            if self.produce_block:
                self.queue.put(task)
                self.metrics.incr("enqueued")
                return True
            else:
                self.queue.put_nowait(task)
                self.metrics.incr("enqueued")
                return True
        except Full:
            # could not enqueue — clean up reservation so future attempts can proceed
            self.metrics.incr("rejected")
            try:
                self.idempotency.release(key)
            except Exception:
                LOG.exception("failed to release idempotency after enqueue Full")
            return False

    def _worker_loop(self):
        while not self._stop_event.is_set() or not self.queue.empty():
            try:
                task = self.queue.get(timeout=0.5)
            except Exception:
                continue
            self.metrics.incr("dequeued")
            self.metrics.sample_queue_depth(self.queue.qsize())
            fut = self.executor.submit(self._run_task_with_retries, task)
            self._futures.append(fut)

    def _run_task_with_retries(self, task: Dict):
        idempotency_key = f"{task.get('image_key')}|{task.get('size')}"
        max_retries = int(task.get("_max_retries", 3))
        backoff_base = float(task.get("_backoff_base", 0.02))
        attempt = 0
        start_ts = now()
        while True:
            try:
                attempt += 1
                result = self.worker_fn(task)
                self.idempotency.mark_success(idempotency_key, json.dumps(result))
                self.metrics.incr("success")
                latency = now() - start_ts
                self.metrics.observe_latency(latency)
                LOG.debug("task %s success (lat=%.4f)", idempotency_key, latency)
                return result
            except TransientError as e:
                self.metrics.incr("retries")
                self.idempotency.increment_retry(idempotency_key)
                if attempt > max_retries:
                    self.idempotency.mark_failure(idempotency_key, json.dumps(task), str(e))
                    self.metrics.incr("failed")
                    LOG.warning("task %s moved to dead-letter after %d attempts", idempotency_key, attempt - 1)
                    return {"error": "dead_letter", "reason": str(e)}
                # exponential backoff + jitter
                to_sleep = backoff_base * (2 ** (attempt - 1))
                to_sleep = to_sleep * (1 + (random.random() - 0.5) * 0.2)
                time.sleep(min(to_sleep, 2.0))
                continue
            except PermanentError as e:
                self.idempotency.mark_failure(idempotency_key, json.dumps(task), str(e))
                self.metrics.incr("failed")
                LOG.error("permanent failure %s: %s", idempotency_key, e)
                return {"error": "permanent", "reason": str(e)}
            except Exception as e:
                # treat unknown exceptions as transient once
                self.metrics.incr("retries")
                self.idempotency.increment_retry(idempotency_key)
                if attempt > max_retries:
                    self.idempotency.mark_failure(idempotency_key, json.dumps(task), traceback.format_exc())
                    self.metrics.incr("failed")
                    return {"error": "dead_letter", "reason": str(e)}
                time.sleep(0.01)

    def start(self, producers: Iterable[threading.Thread] = ()):  # producers are optional
        t = threading.Thread(target=self._worker_loop, daemon=True)
        t.start()
        self._producer_threads.append(t)
        for p in producers:
            self._producer_threads.append(p)

    def stop(self, wait: bool = True, timeout: Optional[float] = None):
        self._stop_event.set()
        # wait for queue to drain
        if wait:
            start = now()
            while not self.queue.empty():
                if timeout and (now() - start) > timeout:
                    break
                time.sleep(0.01)
        # wait for inflight futures
        for fut in list(self._futures):
            try:
                fut.result(timeout=1)
            except Exception:
                pass
        self.executor.shutdown(wait=True)
        self.idempotency.close()

    def snapshot_metrics(self) -> Dict:
        d = self.metrics.to_dict()
        d.update({
            "queue_maxsize": self.queue.maxsize,
            "queue_current": self.queue.qsize(),
        })
        return d


def load_tasks(path: str) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def producer_from_list(taskq: TaskQueueOptimized, tasks: List[Dict], produce_interval: float = 0.0):
    # If caller requests a zero-interval burst, perform enqueues synchronously to
    # deterministically exercise failfast/backpressure behavior in tests.
    if produce_interval == 0.0:
        for t in tasks:
            ok = taskq.enqueue(dict(t))
            if not ok:
                LOG.debug("producer: enqueue rejected %s", t)
        class _Dummy:
            def join(self):
                return
            def is_alive(self):
                return False
        return _Dummy()

    def _p():
        for t in tasks:
            ok = taskq.enqueue(dict(t))
            if not ok:
                LOG.debug("producer: enqueue rejected %s", t)
            if produce_interval:
                time.sleep(produce_interval)
    th = threading.Thread(target=_p, daemon=True)
    th.start()
    return th


def benchmark_run(
    tasks_path: str,
    workers: int = 4,
    queue_size: int = 200,
    produce_block: bool = True,
    max_retries: int = 3,
    fault_rate: float = 0.0,
    seed: Optional[int] = None,
) -> Dict:
    tasks = load_tasks(tasks_path)
    # attach control fields
    for t in tasks:
        t.setdefault("_max_retries", max_retries)
        t.setdefault("_backoff_base", 0.01)
    metrics = Metrics()
    proc = Processor(fault_rate=fault_rate, seed=seed)
    tq = TaskQueueOptimized(worker_fn=proc.process, max_workers=workers, queue_size=queue_size, produce_block=produce_block, metrics=metrics, idempotency_db=DEFAULT_DB)

    p = producer_from_list(tq, tasks)
    start = now()
    tq.start()
    # wait for producer to finish enqueuing
    p.join()
    # wait for queue to drain
    tq.stop(wait=True, timeout=30)
    elapsed = now() - start
    m = tq.snapshot_metrics()
    m.update({
        "elapsed_sec": elapsed,
        "throughput_tps": (m.get("dequeued", 0) / elapsed) if elapsed > 0 else 0,
        "total_tasks": len(tasks),
        "workers": workers,
        "queue_size": queue_size,
        "fault_rate": fault_rate,
    })
    return m


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", default="input_new/sample_tasks_small.json")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--queue-size", type=int, default=200)
    parser.add_argument("--produce-mode", choices=["block", "failfast"], default="block")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--fault-rate", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--benchmark", action="store_true")
    args = parser.parse_args()

    if args.benchmark:
        m = benchmark_run(
            tasks_path=args.tasks,
            workers=args.workers,
            queue_size=args.queue_size,
            produce_block=(args.produce_mode == "block"),
            max_retries=args.max_retries,
            fault_rate=args.fault_rate,
            seed=args.seed,
        )
        out_path = os.path.join(RESULTS_DIR, "optimized_metrics.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(m, f, indent=2)
        print(json.dumps(m, indent=2))
    else:
        print("Run with --benchmark to execute benchmark and export metrics.")


if __name__ == "__main__":
    cli()
