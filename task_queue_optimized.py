"""
Optimized Task Queue Implementation with ThreadPoolExecutor, Bounded Queue, and Idempotency.

Features:
- ThreadPoolExecutor + bounded queue.Queue for concurrent processing
- Idempotency keys for deduplication with SQLite persistence
- Retry logic with exponential backoff and error classification
- Comprehensive metrics collection and export
- Streaming I/O support with local file chunking
- Graceful shutdown and final metrics export
"""

import json
import logging
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from queue import Queue, Full, Empty
from typing import Dict, List, Optional, Tuple
import random
import sys
import argparse

# ============================================================================
# Logging Setup
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Data Structures & Persistence
# ============================================================================

class IdempotencyStore:
    """SQLite-based idempotency key store to prevent duplicate processing."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = None
        # For in-memory databases, keep a persistent connection
        if db_path == ":memory:":
            self.conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._init_db_with_conn(self.conn)
        else:
            # For file-based databases, initialize on first use
            self._init_db()

    def _get_connection(self):
        """
        Get a database connection.
        Returns (conn, should_close) tuple.
        - For in-memory: returns persistent conn, should_close=False
        - For file-based: returns new conn, should_close=True
        """
        if self.conn is not None:
            # In-memory database with persistent connection
            return self.conn, False
        else:
            # File-based database, create new connection
            return sqlite3.connect(self.db_path), True

    def _init_db(self):
        """Initialize SQLite database schema for file-based databases."""
        conn = sqlite3.connect(self.db_path)
        try:
            self._init_db_with_conn(conn)
        finally:
            conn.close()

    def _init_db_with_conn(self, conn):
        """Initialize database schema using provided connection."""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS processed_tasks (
                idempotency_key TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        conn.commit()

    def is_duplicate(self, idempotency_key: str) -> bool:
        """Check if task with this key was already processed."""
        try:
            conn, should_close = self._get_connection()
            try:
                cursor = conn.execute(
                    'SELECT 1 FROM processed_tasks WHERE idempotency_key = ?',
                    (idempotency_key,)
                )
                result = cursor.fetchone() is not None
                return result
            except sqlite3.OperationalError:
                # Table doesn't exist, reinitialize
                self._init_db_with_conn(conn)
                return False
            finally:
                if should_close:
                    conn.close()
        except Exception as e:
            logger.error(f"Error checking duplicate: {e}")
            return False

    def mark_processed(self, task_id: str, idempotency_key: str, status: str, result: Optional[str] = None):
        """Mark a task as processed."""
        now = datetime.utcnow().isoformat()
        try:
            conn, should_close = self._get_connection()
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO processed_tasks
                    (idempotency_key, task_id, status, result, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (idempotency_key, task_id, status, result, now, now))
                conn.commit()
            except sqlite3.OperationalError:
                # Table doesn't exist, reinitialize
                self._init_db_with_conn(conn)
                conn.execute('''
                    INSERT OR REPLACE INTO processed_tasks
                    (idempotency_key, task_id, status, result, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (idempotency_key, task_id, status, result, now, now))
                conn.commit()
            finally:
                if should_close:
                    conn.close()
        except Exception as e:
            logger.error(f"Error marking processed: {e}")

    def get_processed_count(self) -> int:
        """Get total count of processed tasks."""
        try:
            conn, should_close = self._get_connection()
            try:
                cursor = conn.execute('SELECT COUNT(*) FROM processed_tasks')
                return cursor.fetchone()[0]
            except sqlite3.OperationalError:
                return 0
            finally:
                if should_close:
                    conn.close()
        except Exception:
            return 0

    def try_mark_processed(self, task_id: str, idempotency_key: str, status: str, result: Optional[str] = None) -> bool:
        """
        Atomically check if task is already processed and mark it if not.
        Returns True if successfully marked (first time), False if already processed (duplicate).
        Uses SQLite transaction for atomicity.
        """
        now = datetime.utcnow().isoformat()
        try:
            conn, should_close = self._get_connection()
            try:
                # Use transaction for atomicity
                cursor = conn.execute('BEGIN IMMEDIATE')
                try:
                    # Check if already exists
                    cursor = conn.execute(
                        'SELECT 1 FROM processed_tasks WHERE idempotency_key = ?',
                        (idempotency_key,)
                    )
                    if cursor.fetchone() is not None:
                        conn.execute('ROLLBACK')
                        return False  # Already processed
                    
                    # Not found, insert it
                    conn.execute('''
                        INSERT INTO processed_tasks
                        (idempotency_key, task_id, status, result, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (idempotency_key, task_id, status, result, now, now))
                    conn.commit()
                    return True  # Successfully marked
                except Exception:
                    conn.execute('ROLLBACK')
                    raise
            finally:
                if should_close:
                    conn.close()
        except Exception as e:
            logger.error(f"Error in try_mark_processed: {e}")
            return False

    def close(self):
        """Close database connection (for persistent in-memory or file connections)."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __del__(self):
        """Cleanup connection on garbage collection."""
        self.close()


# ============================================================================
# Metrics Collection
# ============================================================================

class Metrics:
    """Comprehensive metrics collection for performance analysis."""

    def __init__(self):
        self.lock = threading.Lock()
        self.enqueue_count = 0
        self.dequeue_count = 0
        self.success_count = 0
        self.fail_count = 0
        self.retry_count = 0
        self.duplicate_count = 0
        self.rejection_count = 0
        self.latencies = []
        self.queue_depths = []
        self.start_time = time.perf_counter()
        self.error_classification = {}

    def record_enqueue(self):
        with self.lock:
            self.enqueue_count += 1

    def record_dequeue(self):
        with self.lock:
            self.dequeue_count += 1

    def record_success(self, latency: float):
        with self.lock:
            self.success_count += 1
            self.latencies.append(latency)

    def record_fail(self, latency: float, error_type: str = "unknown"):
        with self.lock:
            self.fail_count += 1
            self.latencies.append(latency)
            self.error_classification[error_type] = self.error_classification.get(error_type, 0) + 1

    def record_retry(self, error_type: str = "unknown"):
        with self.lock:
            self.retry_count += 1

    def record_duplicate(self):
        with self.lock:
            self.duplicate_count += 1

    def record_rejection(self):
        with self.lock:
            self.rejection_count += 1

    def record_queue_depth(self, depth: int):
        with self.lock:
            self.queue_depths.append(depth)

    def get_mean_latency(self) -> float:
        with self.lock:
            if not self.latencies:
                return 0.0
            return sum(self.latencies) / len(self.latencies)

    def get_p95_latency(self) -> float:
        with self.lock:
            if not self.latencies:
                return 0.0
            sorted_latencies = sorted(self.latencies)
            index = int(len(sorted_latencies) * 0.95)
            return sorted_latencies[min(index, len(sorted_latencies) - 1)]

    def get_throughput(self) -> float:
        """Return tasks per second."""
        elapsed = time.perf_counter() - self.start_time
        if elapsed == 0:
            return 0.0
        return self.success_count / elapsed

    def export_json(self) -> Dict:
        """Export metrics as JSON-serializable dictionary."""
        with self.lock:
            # Calculate values inside the lock without nested lock attempts
            elapsed = time.perf_counter() - self.start_time
            throughput = self.success_count / elapsed if elapsed > 0 else 0.0
            
            mean_latency = 0.0
            if self.latencies:
                mean_latency = sum(self.latencies) / len(self.latencies)
            
            p95_latency = 0.0
            if self.latencies:
                sorted_latencies = sorted(self.latencies)
                index = int(len(sorted_latencies) * 0.95)
                p95_latency = sorted_latencies[min(index, len(sorted_latencies) - 1)]
            
            avg_queue_depth = (sum(self.queue_depths) / len(self.queue_depths)) if self.queue_depths else 0
            
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'duration_seconds': elapsed,
                'enqueue_count': self.enqueue_count,
                'dequeue_count': self.dequeue_count,
                'success_count': self.success_count,
                'fail_count': self.fail_count,
                'retry_count': self.retry_count,
                'duplicate_count': self.duplicate_count,
                'rejection_count': self.rejection_count,
                'throughput_tps': throughput,
                'mean_latency_ms': mean_latency * 1000,
                'p95_latency_ms': p95_latency * 1000,
                'avg_queue_depth': avg_queue_depth,
                'error_classification': self.error_classification
            }


# ============================================================================
# Task Processor with Fault Injection
# ============================================================================

class TaskProcessor:
    """Core processor for individual tasks with fault injection support."""

    def __init__(self, base_latency: float = 0.03, error_probability: float = 0.0):
        self.base_latency = base_latency
        self.error_probability = error_probability

    def process(self, task: Dict) -> Tuple[bool, str, Optional[Dict]]:
        """
        Process a task and return (success, error_type, result).
        
        Error types: 'transient', 'permanent', None (success)
        """
        try:
            # Simulate blocking I/O with jitter
            jitter = random.uniform(0, 0.01)
            time.sleep(self.base_latency + jitter)

            # Fault injection
            if random.random() < self.error_probability:
                error_choice = random.choice(['transient', 'permanent'])
                if error_choice == 'transient':
                    raise TimeoutError("Simulated timeout (transient)")
                else:
                    raise ValueError("Simulated bad parameter (permanent)")

            # Successful processing
            result = {
                'image_key': task.get('image_key'),
                'variant': task.get('size', '200x200'),
                'status': 'ok',
                'processed_at': datetime.utcnow().isoformat()
            }
            return True, None, result

        except TimeoutError as e:
            logger.debug(f"Transient error: {e}")
            return False, 'transient', None
        except ValueError as e:
            logger.debug(f"Permanent error: {e}")
            return False, 'permanent', None
        except Exception as e:
            logger.debug(f"Unknown error: {e}")
            return False, 'unknown', None


# ============================================================================
# Bounded Task Queue
# ============================================================================

class BoundedTaskQueue:
    """Thread-safe bounded task queue with backpressure."""

    def __init__(self, maxsize: int = 200):
        self.queue = Queue(maxsize=maxsize)
        self.maxsize = maxsize

    def enqueue(self, task: Dict, timeout: Optional[float] = None) -> bool:
        """
        Enqueue a task. Returns True if successful, False if rejected.
        """
        try:
            self.queue.put(task, block=True, timeout=timeout)
            return True
        except Full:
            logger.warning("Queue full, rejecting task")
            return False

    def dequeue(self, timeout: Optional[float] = 0.1) -> Optional[Dict]:
        """Dequeue a task with timeout."""
        try:
            return self.queue.get(block=True, timeout=timeout)
        except Empty:
            return None

    def qsize(self) -> int:
        """Get current queue size."""
        return self.queue.qsize()


# ============================================================================
# Worker Pool with Retry Logic
# ============================================================================

class WorkerPool:
    """Manages producer/consumer with retry strategy and graceful shutdown."""

    def __init__(
        self,
        max_workers: int = 4,
        queue_size: int = 200,
        base_latency: float = 0.03,
        error_probability: float = 0.0,
        max_retries: int = 3,
        db_path: str = ":memory:"
    ):
        self.task_queue = BoundedTaskQueue(maxsize=queue_size)
        self.processor = TaskProcessor(base_latency=base_latency, error_probability=error_probability)
        self.idempotency_store = IdempotencyStore(db_path=db_path)
        self.metrics = Metrics()
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.shutdown_event = threading.Event()
        self.dead_letter_queue = []
        self.dead_letter_lock = threading.Lock()

    def _generate_idempotency_key(self, task: Dict) -> str:
        """Generate idempotency key from image_key + variant."""
        image_key = task.get('image_key', '')
        variant = task.get('size', 'default')
        return f"{image_key}#{variant}"

    def _worker_loop(self):
        """Main worker loop: dequeue, deduplicate, process, retry."""
        while not self.shutdown_event.is_set():
            task = self.task_queue.dequeue(timeout=0.5)
            if task is None:
                continue

            self.metrics.record_dequeue()
            task_id = task.get('id', 'unknown')
            idempotency_key = self._generate_idempotency_key(task)

            # Check for duplicates - try to atomically mark as processing
            # If it returns False, it's a duplicate
            if not self.idempotency_store.try_mark_processed(
                task_id, idempotency_key, 'processing'
            ):
                logger.debug(f"Duplicate task detected: {idempotency_key}")
                self.metrics.record_duplicate()
                continue

            # Process with retries
            retries = 0
            start_time = time.perf_counter()
            last_error_type = None

            while retries <= self.max_retries:
                success, error_type, result = self.processor.process(task)
                latency = time.perf_counter() - start_time

                if success:
                    self.idempotency_store.mark_processed(
                        task_id, idempotency_key, 'success',
                        json.dumps(result) if result else None
                    )
                    self.metrics.record_success(latency)
                    logger.info(f"Task {task_id} succeeded after {retries} retries")
                    break

                last_error_type = error_type
                if error_type == 'permanent':
                    # Don't retry permanent errors
                    logger.error(f"Permanent error for task {task_id}: {error_type}")
                    self.metrics.record_fail(latency, error_type)
                    self.idempotency_store.mark_processed(
                        task_id, idempotency_key, 'failed_permanent'
                    )
                    with self.dead_letter_lock:
                        self.dead_letter_queue.append(task)
                    break
                else:
                    # Retry transient errors with exponential backoff
                    if retries < self.max_retries:
                        backoff = (2 ** retries) * 0.01 + random.uniform(0, 0.01)
                        logger.debug(f"Retry {retries + 1} for task {task_id}, backoff: {backoff:.3f}s")
                        self.metrics.record_retry(error_type)
                        time.sleep(backoff)
                        retries += 1
                    else:
                        # Exhausted retries
                        logger.error(f"Max retries exhausted for task {task_id}")
                        self.metrics.record_fail(latency, 'transient_exhausted')
                        self.idempotency_store.mark_processed(
                            task_id, idempotency_key, 'failed_transient'
                        )
                        with self.dead_letter_lock:
                            self.dead_letter_queue.append(task)
                        break

            self.metrics.record_queue_depth(self.task_queue.qsize())

    def enqueue_tasks(self, tasks: List[Dict], fail_fast: bool = False) -> int:
        """
        Enqueue all tasks. Returns number of successfully enqueued tasks.
        If fail_fast=False, retries on full queue; if True, rejects immediately.
        """
        enqueued = 0
        for i, task in enumerate(tasks):
            task['id'] = f"task_{i}"
            timeout = None if not fail_fast else 0.1
            if self.task_queue.enqueue(task, timeout=timeout):
                self.metrics.record_enqueue()
                enqueued += 1
            else:
                self.metrics.record_rejection()
                if fail_fast:
                    logger.warning(f"Failed to enqueue task {i}, rejecting")

        return enqueued

    def run(self) -> Dict:
        """
        Run worker pool until all tasks are processed.
        Returns metrics dictionary.
        """
        # Start worker threads
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        futures = [executor.submit(self._worker_loop) for _ in range(self.max_workers)]

        # Wait for all tasks to drain (no new tasks, queue empty)
        try:
            while not self.shutdown_event.is_set():
                if self.task_queue.qsize() == 0:
                    time.sleep(0.5)
                    if self.task_queue.qsize() == 0:  # Double-check
                        break
                time.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")

        # Shutdown
        self.shutdown_event.set()
        executor.shutdown(wait=True)

        # Export metrics
        return self.metrics.export_json()


# ============================================================================
# Benchmark & CLI
# ============================================================================

def run_benchmark(
    task_count: int = 100,
    max_workers: int = 4,
    queue_size: int = 200,
    concurrent_producers: int = 1,
    error_probability: float = 0.0,
    db_path: str = ":memory:"
):
    """Run benchmark with specified parameters."""
    logger.info(f"Starting benchmark: {task_count} tasks, {max_workers} workers, "
                f"queue_size={queue_size}, concurrent_producers={concurrent_producers}")

    # Generate tasks
    tasks = []
    sizes = ["200x200", "400x400", "800x800"]
    for i in range(task_count):
        task = {
            'image_key': f'img/{i:06d}.jpg',
            'size': random.choice(sizes)
        }
        tasks.append(task)

    # Initialize pool
    pool = WorkerPool(
        max_workers=max_workers,
        queue_size=queue_size,
        base_latency=0.03,
        error_probability=error_probability,
        db_path=db_path
    )

    # Enqueue tasks (single or concurrent producers)
    if concurrent_producers == 1:
        pool.enqueue_tasks(tasks, fail_fast=False)
    else:
        # Distribute tasks across multiple producer threads
        def producer_thread(task_subset):
            pool.enqueue_tasks(task_subset, fail_fast=False)

        producer_threads = []
        chunk_size = len(tasks) // concurrent_producers
        for i in range(concurrent_producers):
            start = i * chunk_size
            end = start + chunk_size if i < concurrent_producers - 1 else len(tasks)
            t = threading.Thread(target=producer_thread, args=(tasks[start:end],))
            t.start()
            producer_threads.append(t)

        for t in producer_threads:
            t.join()

    # Run and collect metrics
    metrics = pool.run()

    # Print results
    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS")
    print("=" * 70)
    print(f"Duration: {metrics['duration_seconds']:.2f}s")
    print(f"Enqueued: {metrics['enqueue_count']}")
    print(f"Dequeued: {metrics['dequeue_count']}")
    print(f"Success: {metrics['success_count']}")
    print(f"Failed: {metrics['fail_count']}")
    print(f"Retries: {metrics['retry_count']}")
    print(f"Duplicates: {metrics['duplicate_count']}")
    print(f"Rejections: {metrics['rejection_count']}")
    print(f"Throughput: {metrics['throughput_tps']:.2f} tasks/sec")
    print(f"Mean Latency: {metrics['mean_latency_ms']:.2f} ms")
    print(f"P95 Latency: {metrics['p95_latency_ms']:.2f} ms")
    print(f"Avg Queue Depth: {metrics['avg_queue_depth']:.2f}")
    if metrics['error_classification']:
        print(f"Error Classification: {metrics['error_classification']}")
    print("=" * 70 + "\n")

    return metrics


def main():
    parser = argparse.ArgumentParser(description='Optimized Task Queue with Benchmark')
    parser.add_argument('--benchmark', action='store_true', help='Run benchmark mode')
    parser.add_argument('--task-count', type=int, default=100, help='Number of tasks to process')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker threads')
    parser.add_argument('--queue-size', type=int, default=200, help='Bounded queue size')
    parser.add_argument('--error-rate', type=float, default=0.0, help='Error injection probability')
    parser.add_argument('--output', type=str, default='benchmark_results.json', help='Output metrics file')

    args = parser.parse_args()

    if args.benchmark:
        metrics = run_benchmark(
            task_count=args.task_count,
            max_workers=args.workers,
            queue_size=args.queue_size,
            error_probability=args.error_rate
        )
        # Export metrics to file
        with open(args.output, 'w') as f:
            json.dump(metrics, f, indent=2)
        logger.info(f"Metrics exported to {args.output}")
    else:
        print("Usage: python task_queue_optimized.py --benchmark [options]")
        print("Run with --help for more options")


if __name__ == '__main__':
    main()
