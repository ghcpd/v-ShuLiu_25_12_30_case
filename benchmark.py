"""Benchmark harness that runs baseline and optimized implementations and emits a comparison report.

Produces JSON outputs under ./results/ and prints a short table to stdout.
"""
from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import time
from typing import Dict

RESULTS_DIR = "./results"
os.makedirs(RESULTS_DIR, exist_ok=True)


def run_baseline(tasks_path: str) -> Dict:
    cmd = [sys.executable, "input_new/baseline_task_queue_v2.py"]
    env = os.environ.copy()
    # baseline script expects sample_tasks_small.json in cwd; pass via cwd
    start = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True, cwd="input_new", timeout=120)
    elapsed = time.perf_counter() - start
    out = p.stdout + p.stderr
    # baseline prints a 'done in X sec' line - try to parse
    parsed = {"elapsed_sec": elapsed}
    for line in out.splitlines():
        if line.startswith("done in"):
            try:
                parsed["elapsed_sec_reported"] = float(line.split()[2])
            except Exception:
                pass
    parsed.update({"throughput_tps": 0 if elapsed == 0 else (5 / elapsed), "total_tasks": 5})
    path = os.path.join(RESULTS_DIR, "baseline_metrics.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)
    return parsed


def run_optimized(tasks_path: str) -> Dict:
    cmd = [sys.executable, "task_queue_optimized.py", "--benchmark", "--tasks", tasks_path, "--workers", "6", "--queue-size", "200"]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    out = p.stdout
    # optimized prints JSON to stdout
    parsed = json.loads(out)
    path = os.path.join(RESULTS_DIR, "optimized_metrics.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)
    return parsed


def compare(baseline: Dict, optimized: Dict) -> Dict:
    b_t = baseline.get("elapsed_sec")
    o_t = optimized.get("elapsed_sec")
    result = {
        "baseline_elapsed": b_t,
        "optimized_elapsed": o_t,
        "elapsed_ratio": (o_t / b_t) if b_t and o_t else None,
        "baseline_p95": baseline.get("p95_latency_sec", None),
        "optimized_p95": optimized.get("p95_latency_sec", None),
    }
    return result


def main():
    tasks = "input_new/sample_tasks_small.json"
    print("Running baseline...")
    b = run_baseline(tasks)
    print("Running optimized...")
    o = run_optimized(tasks)
    cmp = compare(b, o)
    print("\nSummary:")
    print(f" baseline elapsed: {b['elapsed_sec']:.4f}s")
    print(f" optimized elapsed: {o['elapsed_sec']:.4f}s")
    if cmp.get("elapsed_ratio"):
        print(f" elapsed_ratio (opt/baseline): {cmp['elapsed_ratio']:.2f}")
    print("Detailed metrics written to ./results/")


if __name__ == '__main__':
    main()
