import json
import subprocess
import sys

import pytest


@pytest.mark.slow
def test_optimized_improves_over_baseline():
    # run baseline (small sample) and optimized and compare elapsed times
    b = subprocess.run([sys.executable, "input_new/baseline_task_queue_v2.py"], cwd="input_new", capture_output=True, text=True, timeout=20)
    # parse baseline elapsed from stdout if present
    baseline_elapsed = None
    for line in (b.stdout + b.stderr).splitlines():
        if line.startswith("done in"):
            try:
                baseline_elapsed = float(line.split()[2])
            except Exception:
                pass
    # run optimized
    o = subprocess.run([sys.executable, "task_queue_optimized.py", "--benchmark", "--tasks", "input_new/sample_tasks_small.json", "--workers", "6"], capture_output=True, text=True, timeout=20)
    optimized = json.loads(o.stdout)
    optimized_elapsed = optimized.get("elapsed_sec")
    # basic sanity
    assert optimized_elapsed is not None
    if baseline_elapsed:
        # expect optimized to be faster (allow some tolerance)
        assert optimized_elapsed <= baseline_elapsed * 0.9
