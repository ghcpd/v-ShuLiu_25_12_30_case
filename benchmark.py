"""Thin wrapper to run optimized benchmark from README/test scripts."""
from task_queue_optimized import run_benchmark

if __name__ == '__main__':
    run_benchmark('input_new/sample_tasks_small.json', total_tasks=500, workers=8, queue_size=200, block_on_full=True)
