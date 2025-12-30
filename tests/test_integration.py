import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "input_new"
BASELINE_SCRIPT = INPUT_DIR / "baseline_task_queue_v2.py"
OPT_SCRIPT = ROOT / "task_queue_optimized.py"
SAMPLE = INPUT_DIR / "sample_tasks_small.json"


def run_baseline_and_get_time(total_tasks=200, workers=4):
    # Baseline expects to be run from input_new to find sample_tasks_small.json
    env = os.environ.copy()
    start = time.perf_counter()
    proc = subprocess.run([sys.executable, str(BASELINE_SCRIPT)], cwd=str(INPUT_DIR), capture_output=True, text=True, timeout=30)
    end = time.perf_counter()
    # parse output like: done in X sec
    out = proc.stdout + proc.stderr
    found = None
    for line in out.splitlines():
        if line.startswith('done in'):
            parts = line.split()
            try:
                found = float(parts[2])
            except Exception:
                pass
    # If the script printed timing, use it; otherwise fallback to measured
    return found if found is not None else (end - start)


def run_optimized_and_metrics(total_tasks=200, workers=8, queue_size=200):
    proc = subprocess.run([sys.executable, str(OPT_SCRIPT), '--benchmark', '--tasks-file', str(SAMPLE), '--total', str(total_tasks), '--workers', str(workers), '--queue-size', str(queue_size)], capture_output=True, text=True, timeout=60)
    out = proc.stdout.strip()
    # optimized prints a JSON blob
    try:
        metrics = json.loads(out.splitlines()[-1])
    except Exception:
        raise AssertionError(f"Optimized did not print JSON metrics; stdout=\n{out}\nstderr=\n{proc.stderr}")
    return metrics


@pytest.mark.integration
def test_optimized_vs_baseline_small_workload():
    # Part A: duplicate-heavy workload => optimized should deduplicate and only process unique keys
    opt_dup = run_optimized_and_metrics(total_tasks=100, workers=8, queue_size=100)
    # sample_tasks_small.json contains 5 unique entries
    assert opt_dup['success'] == 5
    assert opt_dup['dead_letter'] == 0

    # Part B: unique workload comparison (create 100 unique tasks temporarily)
    original = SAMPLE.read_text()
    try:
        unique_tasks = [{"image_key": f"img/unique-{i}.jpg", "size": "200x200"} for i in range(100)]
        SAMPLE.write_text(json.dumps(unique_tasks))

        baseline_time = run_baseline_and_get_time(total_tasks=100, workers=4)
        opt_unique = run_optimized_and_metrics(total_tasks=100, workers=8, queue_size=100)

        assert opt_unique['success'] == 100
        assert opt_unique['dead_letter'] == 0

        # compare durations (optimized should be meaningfully faster)
        if baseline_time and baseline_time > 0:
            opt_duration = opt_unique['duration_s']
            baseline_throughput = 100.0 / baseline_time
            opt_throughput = opt_unique['throughput_qps']
            # expect at least 1.5x throughput improvement in this environment
            assert opt_throughput >= baseline_throughput * 1.5
    finally:
        SAMPLE.write_text(original)
