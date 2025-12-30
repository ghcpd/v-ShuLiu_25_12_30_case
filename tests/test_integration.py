"""Integration tests for complete task queue workflow"""

import pytest
import json
import time
import tempfile
import os
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from task_queue_optimized import (
    WorkerPool,
    TaskProcessor,
    IdempotencyManager,
    ErrorType
)


class TestWorkerPoolIntegration:
    """Integration tests for WorkerPool"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    
    @pytest.fixture
    def sample_tasks(self):
        """Sample tasks for testing"""
        return [
            {'image_key': 'img/a.jpg', 'size': '200x200'},
            {'image_key': 'img/b.jpg', 'size': '400x400'},
            {'image_key': 'img/c.jpg', 'size': '200x200'},
        ]
    
    def test_complete_workflow(self, temp_dir, sample_tasks):
        """Test complete workflow: submit, process, shutdown"""
        db_path = os.path.join(temp_dir, 'idempotency.db')
        metrics_path = os.path.join(temp_dir, 'metrics.json')
        
        pool = WorkerPool(max_workers=2, queue_size=50)
        pool.idempotency_mgr = IdempotencyManager(db_path)
        
        pool.start()
        
        # Submit tasks
        for task in sample_tasks:
            key = f"{task['image_key']}_{task.get('size')}"
            pool.submit_task(task, key)
        
        # Shutdown and wait
        pool.shutdown(wait=True)
        
        # Export metrics
        report = pool.export_metrics(metrics_path)
        
        # Verify results
        assert report['summary']['total_enqueued'] == 3
        assert report['summary']['success_count'] == 3
        assert report['summary']['fail_count'] == 0
        assert report['summary']['throughput_per_sec'] > 0
        
        # Verify metrics file was created
        assert os.path.exists(metrics_path)
    
    def test_idempotency_deduplication(self, temp_dir, sample_tasks):
        """Test that duplicate tasks are deduplicated"""
        db_path = os.path.join(temp_dir, 'idempotency.db')
        
        pool = WorkerPool(max_workers=2, queue_size=50)
        pool.idempotency_mgr = IdempotencyManager(db_path)
        
        pool.start()
        
        # Submit same task twice
        task = sample_tasks[0]
        key = f"{task['image_key']}_{task.get('size')}"
        
        pool.submit_task(task, key)
        pool.submit_task(task, key)
        
        pool.shutdown(wait=True)
        
        # Both should succeed (second one deduplicated)
        snapshot = pool.metrics.snapshot()
        assert snapshot.success_count == 2
        assert snapshot.fail_count == 0
    
    def test_transient_error_retry(self, temp_dir):
        """Test retry on transient errors"""
        db_path = os.path.join(temp_dir, 'idempotency.db')
        
        # Configure processor with transient errors
        processor_config = {
            'base_latency': 0.01,
            'error_probability': 0.0,
            'transient_error_probability': 0.5  # 50% transient errors
        }
        
        pool = WorkerPool(
            max_workers=2,
            queue_size=50,
            processor_config=processor_config
        )
        pool.idempotency_mgr = IdempotencyManager(db_path)
        
        pool.start()
        
        # Submit tasks
        for i in range(10):
            task = {'image_key': f'img/{i}.jpg', 'size': '200x200'}
            key = f"{task['image_key']}_{task.get('size')}"
            pool.submit_task(task, key)
        
        pool.shutdown(wait=True)
        
        # Most should succeed (after retries)
        snapshot = pool.metrics.snapshot()
        success_rate = snapshot.success_count / (snapshot.success_count + snapshot.fail_count)
        
        # With retries, should have high success rate even with transient errors
        assert snapshot.retry_count > 0
        assert success_rate >= 0.8, f"Success rate {success_rate} too low"
    
    def test_permanent_error_handling(self, temp_dir):
        """Test handling of permanent errors"""
        db_path = os.path.join(temp_dir, 'idempotency.db')
        
        # Configure processor with permanent errors
        processor_config = {
            'base_latency': 0.01,
            'error_probability': 0.5,  # 50% permanent errors
            'transient_error_probability': 0.0
        }
        
        pool = WorkerPool(
            max_workers=2,
            queue_size=50,
            processor_config=processor_config
        )
        pool.idempotency_mgr = IdempotencyManager(db_path)
        
        pool.start()
        
        # Submit tasks
        for i in range(10):
            task = {'image_key': f'img/{i}.jpg', 'size': '200x200'}
            key = f"{task['image_key']}_{task.get('size')}"
            pool.submit_task(task, key)
        
        pool.shutdown(wait=True)
        
        # Some should fail (no retries for permanent errors)
        snapshot = pool.metrics.snapshot()
        assert snapshot.fail_count > 0
        assert 'permanent' in snapshot.error_types
    
    def test_queue_capacity_limit(self, temp_dir):
        """Test that queue respects capacity limit"""
        pool = WorkerPool(max_workers=1, queue_size=5)
        pool.idempotency_mgr = IdempotencyManager(os.path.join(temp_dir, 'idem.db'))
        
        pool.start()
        
        # Submit exactly queue_size tasks (should succeed)
        for i in range(5):
            task = {'image_key': f'img/{i}.jpg', 'size': '200x200'}
            key = f"{task['image_key']}_{task.get('size')}"
            pool.submit_task(task, key)
        
        # 6th task should be rejected
        task = {'image_key': 'img/overflow.jpg', 'size': '200x200'}
        with pytest.raises(RuntimeError):
            pool.submit_task(task, 'overflow')
        
        # Verify rejection was recorded
        snapshot = pool.metrics.snapshot()
        assert snapshot.reject_count == 1
        
        pool.shutdown()
    
    def test_performance_improvement(self, temp_dir, sample_tasks):
        """Test that performance metrics are collected and reasonable"""
        db_path = os.path.join(temp_dir, 'idempotency.db')
        metrics_path = os.path.join(temp_dir, 'metrics.json')
        
        # Create more tasks for better metrics
        tasks = [
            {'image_key': f'img/{i}.jpg', 'size': '200x200'}
            for i in range(20)
        ]
        
        pool = WorkerPool(max_workers=4, queue_size=100)
        pool.idempotency_mgr = IdempotencyManager(db_path)
        
        pool.start()
        
        for task in tasks:
            key = f"{task['image_key']}_{task.get('size')}"
            pool.submit_task(task, key)
        
        pool.shutdown(wait=True)
        report = pool.export_metrics(metrics_path)
        
        # Verify reasonable metrics
        assert report['summary']['throughput_per_sec'] > 0
        assert report['latency']['mean_ms'] > 0
        assert report['latency']['p95_ms'] >= report['latency']['mean_ms']
        assert report['latency']['max_ms'] >= report['latency']['p95_ms']
        
        # With 4 workers, should be reasonably fast
        assert report['summary']['duration_sec'] < 2.0


class TestConcurrencyAndStress:
    """Stress tests for concurrency and edge cases"""
    
    def test_high_concurrency(self, tmp_path):
        """Test with high number of concurrent tasks"""
        db_path = str(tmp_path / 'idempotency.db')
        
        pool = WorkerPool(max_workers=8, queue_size=200)
        pool.idempotency_mgr = IdempotencyManager(db_path)
        
        pool.start()
        
        # Submit many tasks
        num_tasks = 100
        for i in range(num_tasks):
            task = {'image_key': f'img/{i}.jpg', 'size': '200x200'}
            key = f"{task['image_key']}_{task.get('size')}"
            try:
                pool.submit_task(task, key)
            except RuntimeError:
                pass  # Queue full - acceptable
        
        pool.shutdown(wait=True)
        
        snapshot = pool.metrics.snapshot()
        # Should process most tasks
        total_processed = snapshot.success_count + snapshot.fail_count
        assert total_processed >= num_tasks * 0.8
    
    def test_empty_task_set(self, tmp_path):
        """Test handling of empty task set"""
        db_path = str(tmp_path / 'idempotency.db')
        
        pool = WorkerPool(max_workers=2, queue_size=50)
        pool.idempotency_mgr = IdempotencyManager(db_path)
        
        pool.start()
        # Don't submit any tasks
        pool.shutdown(wait=True)
        
        snapshot = pool.metrics.snapshot()
        assert snapshot.success_count == 0
        assert snapshot.fail_count == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
