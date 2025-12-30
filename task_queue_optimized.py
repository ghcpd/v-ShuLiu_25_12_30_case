"""Optimized task queue implementation using ThreadPoolExecutor and bounded queue.

Features:
- Bounded queue.Queue with configurable behavior on full (block vs fail-fast)
- ThreadPoolExecutor worker pool
- SQLite-backed idempotency (UNIQUE constraint)
- Retry with exponential backoff + jitter
- Metrics collection: counters, latency mean/P95, queue depth
- Structured logging

Run as:
    python task_queue_optimized.py --demo
    python task_queue_optimized.py --benchmark
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import sqlite3
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from queue import Queue, Full, Empty
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('task_queue')

DEFAULT_DB = 'state.db'
DEFAULT_QUEUE_SIZE = 200
DEFAULT_WORKERS = 6

# --- Data structures ---
@dataclass
class Task:
    id: str
    image_key: str
    size: str = '200x200'
    attempts: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    idempotency_key: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.idempotency_key:
            self.idempotency_key = f"{self.image_key}:{self.size}"


# --- Persistence for idempotency ---
class IdempotencyStore:
    """Provides a simple claim/processed mechanism using SQLite.
    """
    def __init__(self, path: str = DEFAULT_DB):
        self.path = path
        self._lock = threading.Lock()
        conn = sqlite3.connect(self.path)
        conn.execute("""CREATE TABLE IF NOT EXISTS processed (
            idempotency_key TEXT PRIMARY KEY,
            processed_at REAL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS claims (
            idempotency_key TEXT PRIMARY KEY,
            claimed_at REAL
        )""")
        conn.commit()
        conn.close()

    def claim(self, key: str) -> bool:
        """Attempt to claim a key so duplicates are rejected at enqueue time."""
        with self._lock:
            conn = sqlite3.connect(self.path)
            try:
                conn.execute("INSERT INTO claims (idempotency_key, claimed_at) VALUES (?, ?)", (key, time.time()))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
            finally:
                conn.close()

    def mark_processed(self, key: str):
        with self._lock:
            conn = sqlite3.connect(self.path)
            try:
                # move from claims -> processed
                conn.execute("DELETE FROM claims WHERE idempotency_key = ?", (key,))
                conn.execute("INSERT OR IGNORE INTO processed (idempotency_key, processed_at) VALUES (?, ?)", (key, time.time()))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
            finally:
                conn.close()

    def is_processed(self, key: str) -> bool:
        with self._lock:
            conn = sqlite3.connect(self.path)
            cur = conn.execute("SELECT 1 FROM processed WHERE idempotency_key = ?", (key,))
            found = cur.fetchone() is not None
            conn.close()
            return found


# --- Metrics ---
class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.counters = {
            'enqueued': 0,
            'dequeued': 0,
            'success': 0,
            'failed': 0,
            'retries': 0,
            'rejected': 0,
        }
        self.latencies: List[float] = []
        self.queue_depth_samples: List[int] = []

    def incr(self, name: str, amount: int = 1):
        with self.lock:
            self.counters[name] = self.counters.get(name, 0) + amount

    def observe_latency(self, v: float):
        with self.lock:
            self.latencies.append(v)

    def sample_queue(self, depth: int):
        with self.lock:
            self.queue_depth_samples.append(depth)

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            lat = list(self.latencies)
            qd = list(self.queue_depth_samples)
            counters = dict(self.counters)
        mean = statistics.mean(lat) if lat else 0.0
        p95 = statistics.quantiles(lat, n=100)[94] if len(lat) >= 100 else (sorted(lat)[int(len(lat)*0.95)-1] if lat else 0.0)
        return {
            'counters': counters,
            'latency_mean': mean,
            'latency_p95': p95,
            'queue_depth_samples': qd[-10:],
        }

    def to_json(self) -> str:
        return json.dumps(self.snapshot(), indent=2)


# --- Processor ---
class ProcessingError(Exception):
    pass


class TransientError(ProcessingError):
    pass


class PermanentError(ProcessingError):
    pass


class Processor:
    def __init__(self, storage_dir: str = 'storage'):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

    def process(self, task: Task) -> Dict[str, Any]:
        """Process a task. Simulates streaming read and write. Supports fault injection via task.metadata."""
        # Idempotent processing: write an "output" file in chunks
        _simulate = task.metadata.get('simulate', {})
        # Fault injection: transient or permanent errors
        if _simulate.get('permanent_error'):
            raise PermanentError('bad params')
        if _simulate.get('transient_error') and random.random() < _simulate.get('prob', 0.5):
            raise TransientError('transient failure')

        # Simulate streaming I/O by writing file in chunks and small sleeps
        output_path = os.path.join(self.storage_dir, f"{task.id}.out")
        os.makedirs(self.storage_dir, exist_ok=True)
        chunk_count = _simulate.get('chunks', 4)
        chunk_sleep = _simulate.get('chunk_sleep', 0.01)
        with open(output_path, 'wb') as f:
            for _ in range(chunk_count):
                f.write(b'0' * 1024)  # 1KB chunk
                f.flush()
                time.sleep(chunk_sleep)

        return {'status': 'ok', 'output_path': output_path}


# --- Task Queue & Worker Pool ---
class TaskQueue:
    def __init__(self, max_workers: int = DEFAULT_WORKERS, queue_size: int = DEFAULT_QUEUE_SIZE, block_on_full: bool = True):
        self.queue: Queue = Queue(maxsize=queue_size)
        self.max_workers = max_workers
        self.block_on_full = block_on_full
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.metrics = Metrics()
        self.idemp = IdempotencyStore()
        self.processor = Processor()
        self._stop_event = threading.Event()
        self._futures = []
        self._consumer_thread = threading.Thread(target=self._consumer_loop, daemon=True)

    def start(self):
        logger.info('Starting consumer thread and worker pool')
        self._consumer_thread.start()

    def stop(self, wait: bool = True):
        logger.info('Stopping queue')
        self._stop_event.set()
        if wait:
            # Wait for queue to drain
            while not self.queue.empty():
                time.sleep(0.01)
            # Wait for executor tasks
            for fut in self._futures:
                fut.result()
            self.executor.shutdown(wait=True)
            logger.info('Worker pool shutdown complete')

    def enqueue(self, task: Task) -> bool:
        # Fast path: already processed
        if self.idemp.is_processed(task.idempotency_key):
            logger.debug('Task %s already processed (idempotent)', task.id)
            return True
        # Try to claim the idempotency key so duplicates are rejected immediately
        claimed = self.idemp.claim(task.idempotency_key)
        if not claimed:
            logger.debug('Task %s duplicate claim detected, skipping enqueue', task.id)
            return True
        try:
            if self.block_on_full:
                self.queue.put(task, block=True, timeout=2)
            else:
                self.queue.put(task, block=False)
            self.metrics.incr('enqueued')
            return True
        except Full:
            logger.warning('Queue full, rejecting task %s', task.id)
            self.metrics.incr('rejected')
            # release claim so others can try
            with self.idemp._lock:
                conn = sqlite3.connect(self.idemp.path)
                conn.execute("DELETE FROM claims WHERE idempotency_key = ?", (task.idempotency_key,))
                conn.commit()
                conn.close()
            return False

    def _consumer_loop(self):
        logger.info('Consumer loop started')
        while not self._stop_event.is_set():
            try:
                task: Task = self.queue.get(timeout=0.2)
            except Empty:
                continue
            self.metrics.incr('dequeued')
            # sample queue depth
            self.metrics.sample_queue(self.queue.qsize())
            fut = self.executor.submit(self._process_with_retries, task)
            self._futures.append(fut)

    def _process_with_retries(self, task: Task):
        start = time.perf_counter()
        try:
            # Check idempotency once more before processing
            if self.idemp.is_processed(task.idempotency_key):
                logger.debug('Pre-processed %s', task.id)
                return
            result = self._attempt(task)
            self.metrics.incr('success')
            self.idemp.mark_processed(task.idempotency_key)
            return result
        except PermanentError as e:
            logger.error('Permanent failure for %s: %s', task.id, e)
            self.metrics.incr('failed')
        except Exception as e:
            logger.exception('Unexpected error for %s: %s', task.id, e)
            self.metrics.incr('failed')
        finally:
            elapsed = time.perf_counter() - start
            self.metrics.observe_latency(elapsed)

    def _attempt(self, task: Task):
        attempt = 0
        while True:
            try:
                attempt += 1
                task.attempts = attempt
                return self.processor.process(task)
            except TransientError as e:
                logger.warning('Transient error on %s attempt %d: %s', task.id, attempt, e)
                self.metrics.incr('retries')
                if attempt >= task.max_retries:
                    logger.error('Exceeded retries for %s', task.id)
                    raise
                backoff = (2 ** attempt) * 0.01
                jitter = random.uniform(0, backoff * 0.5)
                time.sleep(backoff + jitter)
            except PermanentError:
                raise

    def export_metrics(self, path: Optional[str] = None) -> Dict[str, Any]:
        snap = self.metrics.snapshot()
        if path:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(snap, f, indent=2)
        return snap


# --- Simple demo / benchmark harness ---
def load_tasks_from_file(path: str) -> List[Task]:
    with open(path, 'r', encoding='utf-8') as f:
        arr = json.load(f)
    out = []
    for i, t in enumerate(arr):
        out.append(Task(id=str(i), image_key=t.get('image_key', f'img_{i}'), size=t.get('size', '200x200')))
    return out


def run_demo(task_file: str, workers: int = DEFAULT_WORKERS, queue_size: int = 100):
    tasks = load_tasks_from_file(task_file)
    tq = TaskQueue(max_workers=workers, queue_size=queue_size)
    tq.start()
    start = time.perf_counter()
    for task in tasks:
        tq.enqueue(task)
    # wait until queue drained and workers done
    tq.stop(wait=True)
    elapsed = time.perf_counter() - start
    metrics = tq.export_metrics()
    metrics['wall_time'] = elapsed
    print('Demo results:', json.dumps(metrics, indent=2))


def benchmark(task_file: str, workers: int = DEFAULT_WORKERS, queue_size: int = 200, concurrent_producers: int = 10, idempotency_db: str = None):
    # Load tasks and partition among concurrent producers
    tasks = load_tasks_from_file(task_file)
    tq = TaskQueue(max_workers=workers, queue_size=queue_size)
    if idempotency_db:
        tq.idemp = IdempotencyStore(idempotency_db)
    tq.start()

    def producer(items: List[Task]):
        for it in items:
            tq.enqueue(it)

    # split tasks
    parts = [[] for _ in range(concurrent_producers)]
    for i, t in enumerate(tasks):
        parts[i % concurrent_producers].append(t)

    start = time.perf_counter()
    threads = []
    for part in parts:
        th = threading.Thread(target=producer, args=(part,))
        th.start()
        threads.append(th)
    for th in threads:
        th.join()

    # give workers time to finish
    tq.stop(wait=True)
    elapsed = time.perf_counter() - start
    metrics = tq.export_metrics()
    total = metrics['counters']['success'] + metrics['counters']['failed']
    throughput = total / elapsed if elapsed > 0 else 0
    report = {
        'wall_time': elapsed,
        'throughput': throughput,
        'metrics': metrics,
    }
    print(json.dumps(report, indent=2))
    # write metrics
    with open('benchmark_result.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    return report

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--demo', action='store_true')
    parser.add_argument('--benchmark', action='store_true')
    parser.add_argument('--file', default='input_new/sample_tasks_small.json')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS)
    parser.add_argument('--queue-size', type=int, default=DEFAULT_QUEUE_SIZE)
    parser.add_argument('--producers', type=int, default=10)
    parser.add_argument('--idempotency-db', type=str, default=None)
    args = parser.parse_args()

    if args.demo:
        run_demo(args.file, workers=args.workers, queue_size=args.queue_size)
    elif args.benchmark:
        benchmark(args.file, workers=args.workers, queue_size=args.queue_size, concurrent_producers=args.producers, idempotency_db=args.idempotency_db)
    else:
        print('Run with --demo or --benchmark')
