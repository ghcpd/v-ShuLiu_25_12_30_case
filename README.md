# Task Queue Optimization (optimized solution)

This workspace contains an optimized pure-Python task queue (bounded queue + thread pool),
idempotency via SQLite, retry/backoff, streaming I/O simulation, metrics, and tests.

Quickstart (Windows):

1) Create venv
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
2) Install
   python -m pip install -U pip
   pip install -r requirements.txt
3) Run baseline
   python input_new/baseline_task_queue_v2.py
4) Run optimized benchmark
   python task_queue_optimized.py --benchmark
5) Run full test + comparison (PowerShell)
   .\run_tests.ps1

What to look for:
- Throughput (tasks/sec) and latency (mean, P95) improvements
- Observable queue backpressure / rejection events
- Idempotency (duplicates skipped)
- Retry behaviour and dead-lettering

Files of interest:
- `task_queue_optimized.py` — optimized implementation
- `benchmark.py` — harness to compare baseline vs optimized
- `tests/` — unit + integration tests (run with `pytest -q`)
- `input_new/` — baseline and task generators
