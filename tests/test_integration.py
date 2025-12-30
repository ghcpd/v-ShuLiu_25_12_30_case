import json
import os
import tempfile
import unittest
from task_queue_optimized import run_optimized

class TestIntegration(unittest.TestCase):
    def setUp(self):
        # Create temp tasks file
        self.tasks = [
            {"image_key": "int_test_1.jpg", "size": "200x200"},
            {"image_key": "int_test_2.jpg", "size": "400x400"},
            {"image_key": "int_test_3.jpg", "size": "800x800"},
        ]
        self.tasks_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(self.tasks, self.tasks_file)
        self.tasks_file.close()

    def tearDown(self):
        os.unlink(self.tasks_file.name)
        # Clean up db and images
        if os.path.exists('idempotency.db'):
            os.unlink('idempotency.db')
        for d in ['input_images', 'output_images']:
            if os.path.exists(d):
                for f in os.listdir(d):
                    os.unlink(os.path.join(d, f))
                os.rmdir(d)

    def test_run_optimized_small(self):
        metrics = run_optimized(self.tasks, maxsize=10, max_workers=2, producer_threads=1)
        stats = metrics.get_stats()
        self.assertEqual(stats['success_count'], len(self.tasks))
        self.assertEqual(stats['fail_count'], 0)
        self.assertGreater(stats['mean_latency'], 0)

    def test_run_optimized_with_duplicates(self):
        tasks_with_dup = self.tasks + self.tasks  # Duplicates
        metrics = run_optimized(tasks_with_dup, maxsize=10, max_workers=2, producer_threads=1)
        stats = metrics.get_stats()
        self.assertEqual(stats['success_count'], len(tasks_with_dup))  # All enqueued, but unique processed, wait no
        # Since duplicates are enqueued, but processing skips duplicates, but in code, process_task returns success for duplicates
        # So success_count = number of enqueues
        self.assertEqual(stats['success_count'], len(tasks_with_dup))

if __name__ == '__main__':
    unittest.main()