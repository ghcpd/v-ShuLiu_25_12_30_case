import json
import os
import tempfile
import time
import unittest
from unittest.mock import patch, MagicMock
import threading

from task_queue_optimized import TaskQueue, Metrics, IdempotencyStore, load_tasks


class TestMetrics(unittest.TestCase):
    def test_metrics_recording(self):
        m = Metrics()
        m.record_enqueue()
        m.record_dequeue()
        m.record_success(0.1)
        m.record_fail()
        m.record_retry()
        m.record_reject()
        m.record_queue_depth(5)

        stats = m.get_stats()
        self.assertEqual(stats['enqueue_count'], 1)
        self.assertEqual(stats['dequeue_count'], 1)
        self.assertEqual(stats['success_count'], 1)
        self.assertEqual(stats['fail_count'], 1)
        self.assertEqual(stats['retry_count'], 1)
        self.assertEqual(stats['reject_count'], 1)
        self.assertEqual(stats['max_queue_depth'], 5)
        self.assertAlmostEqual(stats['mean_latency'], 0.1)


class TestIdempotencyStore(unittest.TestCase):
    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp()
        self.store = IdempotencyStore(self.db_path)

    def tearDown(self):
        self.store.close()
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_idempotency(self):
        key = "test_key"
        self.assertFalse(self.store.is_processed(key))
        self.store.mark_processed(key)
        self.assertTrue(self.store.is_processed(key))


class TestTaskQueue(unittest.TestCase):
    def setUp(self):
        self.tq = TaskQueue(maxsize=10, max_workers=2)

    def tearDown(self):
        self.tq.shutdown()

    def test_enqueue_dequeue(self):
        task = {"image_key": "test.jpg", "size": "200x200"}
        self.assertTrue(self.tq.enqueue(task))
        self.assertEqual(self.tq.metrics.enqueue_count, 1)

    def test_duplicate_enqueue(self):
        task = {"image_key": "test.jpg", "size": "200x200"}
        self.assertTrue(self.tq.enqueue(task))
        self.assertTrue(self.tq.enqueue(task))  # Duplicate, enqueued again
        self.assertEqual(self.tq.metrics.enqueue_count, 2)  # Both enqueued

    @patch('task_queue_optimized.TaskQueue._process_single')
    def test_process_task_success(self, mock_process):
        mock_process.return_value = {"status": "success"}
        task = {"image_key": "test.jpg", "size": "200x200"}
        result = self.tq.process_task(task)
        self.assertEqual(result["status"], "success")
        self.assertEqual(self.tq.metrics.success_count, 1)

    @patch('task_queue_optimized.TaskQueue._process_single')
    def test_process_task_retry_fail(self, mock_process):
        mock_process.side_effect = Exception("Transient error")
        task = {"image_key": "test.jpg", "size": "200x200"}
        with self.assertRaises(Exception):
            self.tq.process_task(task)
        self.assertEqual(self.tq.metrics.fail_count, 1)
        self.assertGreaterEqual(self.tq.metrics.retry_count, 1)


class TestLoadTasks(unittest.TestCase):
    def test_load_tasks(self):
        tasks = [{"image_key": "a.jpg", "size": "200x200"}]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(tasks, f)
            f.flush()
            loaded = load_tasks(f.name)
        os.unlink(f.name)
        self.assertEqual(loaded, tasks)


if __name__ == '__main__':
    unittest.main()