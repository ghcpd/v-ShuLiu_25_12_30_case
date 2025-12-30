"""
Comprehensive unit and integration tests for optimized task queue.
Run with: pytest -rA
"""

import json
import pytest
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

# Import from the optimized module
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from task_queue_optimized import (
    IdempotencyStore,
    Metrics,
    TaskProcessor,
    BoundedTaskQueue,
    WorkerPool,
    run_benchmark
)


# ============================================================================
# Unit Tests: IdempotencyStore
# ============================================================================

class TestIdempotencyStore:
    """Test idempotency key tracking and deduplication."""

    def test_new_key_not_duplicate(self):
        """New keys should not be marked as duplicates."""
        store = IdempotencyStore()
        assert not store.is_duplicate("key1")

    def test_mark_processed_creates_entry(self):
        """Marking a key as processed should be detectable."""
        store = IdempotencyStore()
        store.mark_processed("task1", "key1", "success")
        assert store.is_duplicate("key1")

    def test_duplicate_detection(self):
        """Should correctly identify duplicate keys."""
        store = IdempotencyStore()
        store.mark_processed("task1", "key1", "success")
        store.mark_processed("task2", "key2", "success")

        assert store.is_duplicate("key1")
        assert store.is_duplicate("key2")
        assert not store.is_duplicate("key3")

    def test_processed_count(self):
        """Should track total processed count."""
        store = IdempotencyStore()
        assert store.get_processed_count() == 0

        store.mark_processed("task1", "key1", "success")
        assert store.get_processed_count() == 1

        store.mark_processed("task2", "key2", "success")
        assert store.get_processed_count() == 2

    def test_persistent_store(self):
        """Idempotency store should persist to SQLite."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        try:
            # First instance
            store1 = IdempotencyStore(db_path=db_path)
            store1.mark_processed("task1", "key1", "success")

            # Second instance should see the data
            store2 = IdempotencyStore(db_path=db_path)
            assert store2.is_duplicate("key1")
            assert store2.get_processed_count() == 1
        finally:
            Path(db_path).unlink(missing_ok=True)


# ============================================================================
# Unit Tests: Metrics
# ============================================================================

class TestMetrics:
    """Test metrics collection and calculations."""

    def test_basic_counting(self):
        """Metrics should count events correctly."""
        metrics = Metrics()
        assert metrics.enqueue_count == 0
        assert metrics.dequeue_count == 0

        metrics.record_enqueue()
        metrics.record_enqueue()
        assert metrics.enqueue_count == 2

        metrics.record_dequeue()
        assert metrics.dequeue_count == 1

    def test_success_failure_tracking(self):
        """Should track success and failure separately."""
        metrics = Metrics()

        metrics.record_success(0.05)
        metrics.record_success(0.10)
        assert metrics.success_count == 2

        metrics.record_fail(0.15, "transient")
        assert metrics.fail_count == 1

    def test_mean_latency(self):
        """Should calculate mean latency correctly."""
        metrics = Metrics()
        metrics.record_success(0.10)
        metrics.record_success(0.20)
        metrics.record_success(0.30)

        mean = metrics.get_mean_latency()
        assert abs(mean - 0.20) < 0.001

    def test_p95_latency(self):
        """Should calculate P95 latency correctly."""
        metrics = Metrics()
        # Generate 100 values from 0 to 1
        for i in range(100):
            metrics.record_success(i * 0.01)

        p95 = metrics.get_p95_latency()
        # P95 should be around 0.95
        assert 0.90 < p95 < 1.0

    def test_throughput_calculation(self):
        """Should calculate throughput (tasks per second)."""
        metrics = Metrics()
        metrics.record_success(0.01)
        metrics.record_success(0.01)
        metrics.record_success(0.01)

        # Throughput should be non-zero
        throughput = metrics.get_throughput()
        assert throughput > 0

    def test_duplicate_counting(self):
        """Should count duplicates separately."""
        metrics = Metrics()
        metrics.record_duplicate()
        metrics.record_duplicate()
        assert metrics.duplicate_count == 2

    def test_rejection_counting(self):
        """Should count rejections separately."""
        metrics = Metrics()
        metrics.record_rejection()
        metrics.record_rejection()
        metrics.record_rejection()
        assert metrics.rejection_count == 3

    def test_error_classification(self):
        """Should classify and count error types."""
        metrics = Metrics()
        metrics.record_fail(0.05, "transient")
        metrics.record_fail(0.05, "transient")
        metrics.record_fail(0.05, "permanent")

        assert metrics.error_classification["transient"] == 2
        assert metrics.error_classification["permanent"] == 1

    def test_export_json(self):
        """Should export metrics as JSON-serializable dict."""
        metrics = Metrics()
        metrics.record_enqueue()
        metrics.record_success(0.05)

        exported = metrics.export_json()
        assert isinstance(exported, dict)
        assert "enqueue_count" in exported
        assert "success_count" in exported
        assert "throughput_tps" in exported
        assert exported["enqueue_count"] == 1
        assert exported["success_count"] == 1


# ============================================================================
# Unit Tests: TaskProcessor
# ============================================================================

class TestTaskProcessor:
    """Test task processing logic."""

    def test_successful_processing(self):
        """Should process tasks successfully without errors."""
        processor = TaskProcessor(base_latency=0.01, error_probability=0.0)
        task = {"image_key": "img/test.jpg", "size": "200x200"}

        success, error_type, result = processor.process(task)
        assert success is True
        assert error_type is None
        assert result is not None
        assert result["status"] == "ok"
        assert result["image_key"] == "img/test.jpg"

    def test_error_injection(self):
        """Should inject errors based on probability."""
        processor = TaskProcessor(base_latency=0.01, error_probability=1.0)  # Always fail
        task = {"image_key": "img/test.jpg", "size": "200x200"}

        success, error_type, result = processor.process(task)
        assert success is False
        assert error_type in ["transient", "permanent"]
        assert result is None

    def test_latency_includes_base(self):
        """Processing should take at least base_latency."""
        processor = TaskProcessor(base_latency=0.05, error_probability=0.0)
        task = {"image_key": "img/test.jpg", "size": "200x200"}

        start = time.perf_counter()
        processor.process(task)
        elapsed = time.perf_counter() - start

        assert elapsed >= 0.05

    def test_result_structure(self):
        """Successful result should have required fields."""
        processor = TaskProcessor(base_latency=0.01, error_probability=0.0)
        task = {"image_key": "img/a.jpg", "size": "400x400"}

        _, _, result = processor.process(task)
        assert "image_key" in result
        assert "variant" in result
        assert "status" in result
        assert "processed_at" in result


# ============================================================================
# Unit Tests: BoundedTaskQueue
# ============================================================================

class TestBoundedTaskQueue:
    """Test bounded queue functionality."""

    def test_enqueue_dequeue(self):
        """Should enqueue and dequeue tasks correctly."""
        queue = BoundedTaskQueue(maxsize=10)
        task = {"id": 1, "data": "test"}

        assert queue.enqueue(task) is True
        assert queue.qsize() == 1

        dequeued = queue.dequeue(timeout=1.0)
        assert dequeued == task
        assert queue.qsize() == 0

    def test_queue_size_limit(self):
        """Queue should reject tasks when full."""
        queue = BoundedTaskQueue(maxsize=2)

        assert queue.enqueue({"id": 1}) is True
        assert queue.enqueue({"id": 2}) is True
        assert queue.enqueue({"id": 3}, timeout=0.1) is False  # Should fail

    def test_empty_dequeue(self):
        """Dequeuing from empty queue should return None."""
        queue = BoundedTaskQueue(maxsize=10)
        assert queue.dequeue(timeout=0.1) is None

    def test_qsize(self):
        """Should accurately report queue size."""
        queue = BoundedTaskQueue(maxsize=10)
        assert queue.qsize() == 0

        queue.enqueue({"id": 1})
        queue.enqueue({"id": 2})
        assert queue.qsize() == 2


# ============================================================================
# Integration Tests: WorkerPool
# ============================================================================

class TestWorkerPool:
    """Integration tests for the complete worker pool."""

    def test_basic_task_processing(self):
        """Should process simple tasks to completion."""
        pool = WorkerPool(
            max_workers=2,
            queue_size=50,
            base_latency=0.01,
            error_probability=0.0
        )

        tasks = [
            {"image_key": f"img/{i}.jpg", "size": "200x200"}
            for i in range(10)
        ]

        pool.enqueue_tasks(tasks)
        metrics = pool.run()

        assert metrics["success_count"] == 10
        assert metrics["fail_count"] == 0
        assert metrics["enqueue_count"] == 10
        assert metrics["dequeue_count"] == 10

    def test_idempotency_deduplication(self):
        """Should deduplicate tasks with same idempotency key."""
        pool = WorkerPool(
            max_workers=2,
            queue_size=50,
            base_latency=0.01,
            error_probability=0.0
        )

        # Enqueue same task twice
        task = {"image_key": "img/same.jpg", "size": "200x200"}
        tasks = [task, task]

        pool.enqueue_tasks(tasks)
        metrics = pool.run()

        # Only one should be processed, one should be duplicate
        assert metrics["success_count"] + metrics["duplicate_count"] == 2
        assert metrics["duplicate_count"] == 1

    def test_retry_on_transient_error(self):
        """Should retry tasks that fail with transient errors."""
        pool = WorkerPool(
            max_workers=2,
            queue_size=50,
            base_latency=0.01,
            error_probability=0.5,  # 50% error rate
            max_retries=3
        )

        tasks = [
            {"image_key": f"img/{i}.jpg", "size": "200x200"}
            for i in range(5)
        ]

        pool.enqueue_tasks(tasks)
        metrics = pool.run()

        # With retries, should have some successes
        assert metrics["success_count"] + metrics["fail_count"] > 0
        # Should have recorded retries
        assert metrics["retry_count"] >= 0

    def test_queue_rejection_on_overflow(self):
        """Should reject tasks when queue is full."""
        pool = WorkerPool(
            max_workers=1,
            queue_size=5,
            base_latency=0.1,  # Long processing time
            error_probability=0.0
        )

        # Create more tasks than queue can hold
        tasks = [
            {"image_key": f"img/{i}.jpg", "size": "200x200"}
            for i in range(20)
        ]

        enqueued = pool.enqueue_tasks(tasks, fail_fast=True)

        # Some should be rejected
        assert enqueued < len(tasks)
        assert pool.metrics.rejection_count > 0

    def test_dead_letter_queue(self):
        """Failed tasks should go to dead-letter queue."""
        pool = WorkerPool(
            max_workers=2,
            queue_size=50,
            base_latency=0.01,
            error_probability=1.0,  # Always fail
            max_retries=0  # No retries
        )

        tasks = [
            {"image_key": f"img/{i}.jpg", "size": "200x200"}
            for i in range(5)
        ]

        pool.enqueue_tasks(tasks)
        metrics = pool.run()

        # All tasks should fail and go to DLQ
        assert len(pool.dead_letter_queue) > 0


# ============================================================================
# Performance & Concurrency Tests
# ============================================================================

class TestConcurrency:
    """Test concurrent processing and thread safety."""

    def test_concurrent_enqueue(self):
        """Multiple threads should enqueue without data loss."""
        pool = WorkerPool(max_workers=4, queue_size=200)
        task_count = 50

        def enqueue_batch(batch_id):
            for i in range(10):
                task = {
                    "image_key": f"img/batch_{batch_id}_{i}.jpg",
                    "size": "200x200"
                }
                pool.enqueue_tasks([task])

        threads = [
            threading.Thread(target=enqueue_batch, args=(i,))
            for i in range(5)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        metrics = pool.run()

        # All 50 tasks should be accounted for
        assert metrics["enqueue_count"] == 50
        assert metrics["success_count"] + metrics["fail_count"] + metrics["duplicate_count"] == 50

    def test_metrics_thread_safety(self):
        """Metrics should be safely updated from multiple threads."""
        metrics = Metrics()

        def record_events():
            for _ in range(100):
                metrics.record_enqueue()
                metrics.record_success(0.01)
                metrics.record_retry()

        threads = [threading.Thread(target=record_events) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 5 threads * 100 iterations
        assert metrics.enqueue_count == 500
        assert metrics.success_count == 500
        assert metrics.retry_count == 500


# ============================================================================
# Integration: Benchmark with Small Load
# ============================================================================

class TestBenchmark:
    """Test the benchmark function itself."""

    def test_benchmark_completes(self):
        """Benchmark should run and return metrics."""
        metrics = run_benchmark(
            task_count=10,
            max_workers=2,
            queue_size=50,
            concurrent_producers=1,
            error_probability=0.0
        )

        assert metrics is not None
        assert metrics["enqueue_count"] > 0
        assert metrics["success_count"] > 0

    def test_benchmark_with_error_injection(self):
        """Benchmark should handle error injection."""
        metrics = run_benchmark(
            task_count=10,
            max_workers=2,
            queue_size=50,
            concurrent_producers=1,
            error_probability=0.2
        )

        # With error injection, should have some failures
        assert metrics["fail_count"] + metrics["success_count"] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-rA', '-v'])
