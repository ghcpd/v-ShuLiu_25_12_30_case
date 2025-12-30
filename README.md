# Task Queue Optimizer (Mini Project)

This project implements an optimized task queue using ThreadPoolExecutor and a bounded queue with idempotency and retry logic.

Quick start (Windows PowerShell):

1. Create venv: `python -m venv .venv`
2. Activate: `.venv\Scripts\Activate.ps1`
3. Install: `python -m pip install -U pip; pip install -r requirements.txt`
4. Run baseline: `python input_new/baseline_task_queue_v2.py`
5. Run optimized benchmark: `python task_queue_optimized.py --benchmark --file input_new/sample_tasks_small.json`
6. Run tests: `pytest -rA`

Files:
- `task_queue_optimized.py`: Optimized implementation
- `benchmark.py`: Runs baseline and optimized to compare
- `tests/`: Unit and integration tests

