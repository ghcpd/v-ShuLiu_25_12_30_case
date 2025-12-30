# Task Queue Optimized

This workspace implements an optimized task queue using ThreadPoolExecutor + bounded queue, idempotency, retries and metrics. It is lightweight and uses only standard libraries.

Quick start (Windows PowerShell):

1. Create venv: python -m venv .venv
2. Activate: .venv\Scripts\Activate.ps1
3. Install: python -m pip install -U pip; pip install -r requirements.txt
4. Run baseline: python input_new/baseline_task_queue_v2.py  (run from the `input_new` folder)
5. Run optimized benchmark: python task_queue_optimized.py --benchmark
6. Run tests: pytest -rA

Files of interest:
- `task_queue_optimized.py` — optimized implementation and benchmark
- `benchmark.py` — quick runner for larger benchmark
- `tests/` — unit and integration tests (run with pytest)
