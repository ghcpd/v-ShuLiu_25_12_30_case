import json
import logging
import os
import queue
import random
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any
import statistics

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Metrics:
    def __init__(self):
        self.enqueue_count = 0
        self.dequeue_count = 0
        self.success_count = 0
        self.fail_count = 0
        self.retry_count = 0
        self.reject_count = 0
        self.latencies = []
        self.queue_depths = []
        self.lock = threading.Lock()

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

    def record_fail(self):
        with self.lock:
            self.fail_count += 1

    def record_retry(self):
        with self.lock:
            self.retry_count += 1

    def record_reject(self):
        with self.lock:
            self.reject_count += 1

    def record_queue_depth(self, depth: int):
        with self.lock:
            self.queue_depths.append(depth)

    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            latencies = self.latencies
            mean_latency = statistics.mean(latencies) if latencies else 0
            p95_latency = sorted(latencies)[int(0.95 * len(latencies))] if latencies else 0
            max_depth = max(self.queue_depths) if self.queue_depths else 0
            return {
                'enqueue_count': self.enqueue_count,
                'dequeue_count': self.dequeue_count,
                'success_count': self.success_count,
                'fail_count': self.fail_count,
                'retry_count': self.retry_count,
                'reject_count': self.reject_count,
                'mean_latency': mean_latency,
                'p95_latency': p95_latency,
                'max_queue_depth': max_depth,
                'total_tasks': self.enqueue_count
            }

    def to_json(self) -> str:
        return json.dumps(self.get_stats(), indent=2)

class IdempotencyStore:
    def __init__(self, db_path: str = 'idempotency.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute('CREATE TABLE IF NOT EXISTS processed (key TEXT PRIMARY KEY)')
        self.lock = threading.Lock()

    def close(self):
        self.conn.close()

    def is_processed(self, key: str) -> bool:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute('SELECT 1 FROM processed WHERE key = ?', (key,))
            return cursor.fetchone() is not None

    def mark_processed(self, key: str):
        with self.lock:
            self.conn.execute('INSERT OR IGNORE INTO processed (key) VALUES (?)', (key,))
            self.conn.commit()

class TaskQueue:
    def __init__(self, maxsize: int = 100, max_workers: int = 4):
        self.queue = queue.Queue(maxsize=maxsize)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.metrics = Metrics()
        self.idempotency = IdempotencyStore()
        self.running = True
        self.shutdown_event = threading.Event()

    def enqueue(self, task: Dict) -> bool:
        idempotency_key = f"{task['image_key']}_{task.get('size', '200x200')}"
        if self.idempotency.is_processed(idempotency_key):
            logging.info(f"Duplicate task skipped: {idempotency_key}")
            return True  # Consider as success
        try:
            self.queue.put(task, timeout=1)  # Block briefly
            self.metrics.record_enqueue()
            self.metrics.record_queue_depth(self.queue.qsize())
            return True
        except queue.Full:
            self.metrics.record_reject()
            logging.warning(f"Queue full, rejected task: {task}")
            return False

    def process_task(self, task: Dict) -> Dict:
        start_time = time.perf_counter()
        idempotency_key = f"{task['image_key']}_{task.get('size', '200x200')}"
        retries = 0
        max_retries = 3
        backoff = 0.1

        while retries <= max_retries:
            try:
                result = self._process_single(task)
                elapsed = time.perf_counter() - start_time
                self.metrics.record_success(elapsed)
                self.idempotency.mark_processed(idempotency_key)
                return result
            except Exception as e:
                error_type = self._classify_error(e)
                if error_type == 'transient' and retries < max_retries:
                    retries += 1
                    self.metrics.record_retry()
                    sleep_time = backoff * (2 ** retries) + random.uniform(0, 0.1)
                    time.sleep(sleep_time)
                    logging.info(f"Retrying task {task} after {sleep_time:.2f}s")
                else:
                    elapsed = time.perf_counter() - start_time
                    self.metrics.record_fail()
                    logging.error(f"Failed task {task} after {retries} retries: {e}")
                    raise

    def _process_single(self, task: Dict) -> Dict:
        # Simulate streaming I/O
        image_key = task['image_key']
        size = task.get('size', '200x200')
        # Create dummy input file if not exists
        input_dir = 'input_images'
        os.makedirs(input_dir, exist_ok=True)
        input_path = os.path.join(input_dir, image_key.replace('/', '_'))
        if not os.path.exists(input_path):
            with open(input_path, 'wb') as f:
                f.write(b'dummy image data' * 100)  # 1600 bytes

        # Read in chunks
        with open(input_path, 'rb') as f:
            data = b''
            while chunk := f.read(1024):
                data += chunk
                time.sleep(0.001)  # Simulate processing

        # Simulate processing time
        time.sleep(0.02 + random.uniform(0, 0.01))  # Base 20ms + jitter

        # Write output
        output_dir = 'output_images'
        os.makedirs(output_dir, exist_ok=True)
        output_key = f"{image_key}_{size}.processed"
        output_path = os.path.join(output_dir, output_key.replace('/', '_'))
        with open(output_path, 'wb') as f:
            f.write(data[:100])  # Dummy processed data

        return {
            'image_key': image_key,
            'variant': size,
            'status': 'success',
            'output_key': output_key
        }

    def _classify_error(self, e: Exception) -> str:
        # Simple classification: assume all are transient for demo
        return 'transient'

    def worker(self):
        while not self.shutdown_event.is_set():
            try:
                task = self.queue.get(timeout=1)
                self.metrics.record_dequeue()
                self.metrics.record_queue_depth(self.queue.qsize())
                future = self.executor.submit(self.process_task, task)
                future.result()  # Wait for completion
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Worker error: {e}")

    def start_workers(self, num_workers: int):
        for _ in range(num_workers):
            threading.Thread(target=self.worker, daemon=True).start()

    def shutdown(self):
        self.running = False
        self.shutdown_event.set()
        self.executor.shutdown(wait=True)
        self.idempotency.conn.close()

def load_tasks(path: str) -> List[Dict]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def run_optimized(tasks: List[Dict], maxsize: int = 100, max_workers: int = 4, producer_threads: int = 10):
    tq = TaskQueue(maxsize=maxsize, max_workers=max_workers)
    tq.start_workers(max_workers)

    def producer():
        for task in tasks:
            while not tq.enqueue(task):
                time.sleep(0.01)  # Wait if queue full

    producers = []
    for _ in range(producer_threads):
        p = threading.Thread(target=producer)
        p.start()
        producers.append(p)

    for p in producers:
        p.join()

    tq.queue.join()  # Wait for all tasks to be processed
    tq.shutdown()
    return tq.metrics

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--benchmark', action='store_true')
    parser.add_argument('--tasks', default='input_new/sample_tasks_small.json')
    args = parser.parse_args()

    tasks = load_tasks(args.tasks)
    start = time.perf_counter()
    metrics = run_optimized(tasks, maxsize=100, max_workers=4, producer_threads=10)
    elapsed = time.perf_counter() - start

    if args.benchmark:
        stats = metrics.get_stats()
        throughput = stats['total_tasks'] / elapsed
        print(f"Throughput: {throughput:.2f} tasks/sec")
        print(f"Mean latency: {stats['mean_latency']:.4f} sec")
        print(f"P95 latency: {stats['p95_latency']:.4f} sec")
        print(f"Error rate: {stats['fail_count'] / stats['total_tasks']:.2%}")
        print(f"Max queue depth: {stats['max_queue_depth']}")
        with open('metrics.json', 'w') as f:
            json.dump(stats, f, indent=2)
    else:
        print(f"Processed in {elapsed:.4f} sec")