"""Unit tests for task queue components"""

import pytest
import json
import sqlite3
import time
import tempfile
import os
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from task_queue_optimized import (
    TaskProcessor,
    IdempotencyManager,
    MetricsCollector,
    TaskQueue,
    TaskRecord,
    TaskStatus,
    ErrorType,
    MetricsSnapshot
)


class TestTaskProcessor:
    """Unit tests for TaskProcessor"""
    
    def test_process_success(self):
        """Test successful task processing"""
        processor = TaskProcessor(base_latency=0.01)
        task = {'image_key': 'img/test.jpg', 'size': '200x200'}
        result = processor.process(task)
        
        assert result['status'] == 'ok'
        assert result['image_key'] == 'img/test.jpg'
        assert result['variant'] == '200x200'
    
    def test_latency_with_jitter(self):
        """Test that processing includes latency"""
        processor = TaskProcessor(base_latency=0.05)
        task = {'image_key': 'img/test.jpg', 'size': '200x200'}
        
        start = time.perf_counter()
        processor.process(task)
        elapsed = time.perf_counter() - start
        
        # Should be approximately 0.05s, with jitter
        assert 0.03 < elapsed < 0.07, f"Latency {elapsed} outside expected range"
    
    def test_transient_error_injection(self):
        """Test transient error (timeout) injection"""
        processor = TaskProcessor(
            base_latency=0.01,
            error_probability=0.0,
            transient_error_probability=1.0
        )
        task = {'image_key': 'img/test.jpg'}
        
        with pytest.raises(TimeoutError):
            processor.process(task)
    
    def test_permanent_error_injection(self):
        """Test permanent error injection"""
        processor = TaskProcessor(
            base_latency=0.01,
            error_probability=1.0,
            transient_error_probability=0.0
        )
        task = {'image_key': 'img/test.jpg'}
        
        with pytest.raises(ValueError):
            processor.process(task)


class TestIdempotencyManager:
    """Unit tests for IdempotencyManager"""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing"""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, 'test.db')
            yield db_path
            # Close any open connections before cleanup
            import gc
            gc.collect()
    
    def test_not_processed_initially(self, temp_db):
        """Test that new key is not marked as processed"""
        mgr = IdempotencyManager(temp_db)
        assert not mgr.is_processed('key1')
    
    def test_mark_and_check_processed(self, temp_db):
        """Test marking key as processed"""
        mgr = IdempotencyManager(temp_db)
        output = {'status': 'ok'}
        
        mgr.mark_processed('key1', output)
        assert mgr.is_processed('key1')
    
    def test_get_cached_output(self, temp_db):
        """Test retrieving cached output"""
        mgr = IdempotencyManager(temp_db)
        output = {'status': 'ok', 'result': 'test'}
        
        mgr.mark_processed('key1', output)
        cached = mgr.get_output('key1')
        
        assert cached == output
    
    def test_multiple_keys(self, temp_db):
        """Test handling multiple idempotency keys"""
        mgr = IdempotencyManager(temp_db)
        
        mgr.mark_processed('key1', {'data': '1'})
        mgr.mark_processed('key2', {'data': '2'})
        
        assert mgr.is_processed('key1')
        assert mgr.is_processed('key2')
        assert not mgr.is_processed('key3')
    
    def test_concurrent_duplicate_handling(self, temp_db):
        """Test concurrent marking of same key (no error)"""
        mgr = IdempotencyManager(temp_db)
        output = {'status': 'ok'}
        
        # Mark twice - should not raise error
        mgr.mark_processed('key1', output)
        mgr.mark_processed('key1', output)
        
        assert mgr.is_processed('key1')


class TestMetricsCollector:
    """Unit tests for MetricsCollector"""
    
    def test_initial_metrics(self):
        """Test initial metrics state"""
        metrics = MetricsCollector()
        snapshot = metrics.snapshot()
        
        assert snapshot.enqueue_count == 0
        assert snapshot.success_count == 0
        assert snapshot.fail_count == 0
    
    def test_record_enqueue(self):
        """Test recording enqueue"""
        metrics = MetricsCollector()
        metrics.record_enqueue('task1', 'key1')
        
        snapshot = metrics.snapshot()
        assert snapshot.enqueue_count == 1
    
    def test_record_success(self):
        """Test recording successful task"""
        metrics = MetricsCollector()
        metrics.record_enqueue('task1', 'key1')
        metrics.record_dequeue('task1')
        
        # Simulate processing time
        time.sleep(0.01)
        
        metrics.record_success('task1', {'status': 'ok'})
        
        snapshot = metrics.snapshot()
        assert snapshot.success_count == 1
        assert len(snapshot.latencies) > 0
    
    def test_record_failure(self):
        """Test recording failed task"""
        metrics = MetricsCollector()
        metrics.record_enqueue('task1', 'key1')
        metrics.record_dequeue('task1')
        
        error = ValueError("Test error")
        metrics.record_failure('task1', error, ErrorType.PERMANENT)
        
        snapshot = metrics.snapshot()
        assert snapshot.fail_count == 1
        assert 'permanent' in snapshot.error_types
    
    def test_record_retry(self):
        """Test recording retry"""
        metrics = MetricsCollector()
        metrics.record_enqueue('task1', 'key1')
        
        metrics.record_retry('task1')
        
        snapshot = metrics.snapshot()
        assert snapshot.retry_count == 1
    
    def test_percentile_calculation(self):
        """Test latency percentile calculation"""
        metrics = MetricsCollector()
        
        # Manually add latencies
        metrics.latencies = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        
        snapshot = metrics.snapshot()
        # P95 should be around 95
        p95 = MetricsCollector._percentile(snapshot.latencies, 95)
        assert 90 <= p95 <= 100
    
    def test_thread_safety(self):
        """Test concurrent access to metrics (basic check)"""
        import threading
        
        metrics = MetricsCollector()
        
        def record_events():
            for i in range(100):
                metrics.record_enqueue(f'task{i}', f'key{i}')
                metrics.record_success(f'task{i}', {})
        
        threads = [threading.Thread(target=record_events) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        snapshot = metrics.snapshot()
        assert snapshot.success_count == 500


class TestTaskRecord:
    """Unit tests for TaskRecord"""
    
    def test_task_record_creation(self):
        """Test task record creation"""
        record = TaskRecord(task_id='task1', idempotency_key='key1')
        
        assert record.task_id == 'task1'
        assert record.idempotency_key == 'key1'
        assert record.status == TaskStatus.QUEUED
        assert record.retries == 0
    
    def test_duration_calculation(self):
        """Test duration calculation"""
        record = TaskRecord(task_id='task1', idempotency_key='key1')
        
        time.sleep(0.01)
        record.completed_at = time.perf_counter()
        
        duration = record.duration_ms()
        assert duration >= 10  # At least 10ms


class TestTaskQueue:
    """Unit tests for TaskQueue (basic submission)"""
    
    def test_queue_creation(self):
        """Test task queue creation"""
        queue = TaskQueue(max_workers=2, queue_size=50)
        
        assert queue.max_workers == 2
        assert queue.queue_size == 50
        assert queue.get_queue_depth() == 0
    
    def test_submit_task(self):
        """Test submitting a task"""
        queue = TaskQueue(max_workers=2, queue_size=50)
        
        task = {'image_key': 'img/test.jpg', 'size': '200x200'}
        task_id = queue.submit(task, 'key1')
        
        assert task_id.startswith('task-')
        assert queue.get_queue_depth() == 1
        
        queue.shutdown()
    
    def test_queue_overflow_rejection(self):
        """Test queue rejection on overflow"""
        queue = TaskQueue(max_workers=1, queue_size=2)
        
        task = {'image_key': 'img/test.jpg', 'size': '200x200'}
        
        # Fill queue
        queue.submit(task, 'key1')
        queue.submit(task, 'key2')
        
        # Third should be rejected
        with pytest.raises(RuntimeError):
            queue.submit(task, 'key3')
        
        metrics = queue.metrics.snapshot()
        assert metrics.reject_count == 1
        
        queue.shutdown()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
