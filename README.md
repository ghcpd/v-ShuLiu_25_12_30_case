# Task Queue Optimization

This project optimizes a task queue system for image processing using Python's ThreadPoolExecutor and bounded queues.

## Setup

1. Create virtual environment:
   ```bash
   python -m venv .venv
   ```

2. Activate (Windows):
   ```powershell
   .venv\Scripts\Activate.ps1
   ```

3. Install dependencies:
   ```bash
   pip install -U pip
   pip install -r requirements.txt
   ```

## Running

- Run baseline: `python input_new/baseline_task_queue_v2.py`
- Run optimized: `python task_queue_optimized.py --benchmark`
- Run tests: `pytest -rA`
- Run benchmark: `python benchmark.py 1000 50`

## Performance Comparison

Baseline: ~0.15s for 5 tasks
Optimized: ~116 tasks/sec throughput

## Features

- Bounded queue with backpressure
- Idempotency using SQLite
- Exponential backoff retries
- Metrics collection
- Streaming I/O simulation