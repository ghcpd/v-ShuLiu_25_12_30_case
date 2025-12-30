"""Run baseline and optimized benchmarks and compare results."""
import json
import subprocess
import time
import sys

BASELINE_SCRIPT = 'input_new/baseline_task_queue_v2.py'
OPT_SCRIPT = 'task_queue_optimized.py'


def run_baseline(task_file: str, workers: int = 4) -> dict:
    # baseline script prints total time as 'done in X sec', capture stdout
    proc = subprocess.run([sys.executable, BASELINE_SCRIPT], capture_output=True, text=True)
    out = proc.stdout + proc.stderr
    # extract the last number
    t = None
    for line in out.splitlines()[::-1]:
        if 'done in' in line:
            try:
                t = float(line.strip().split()[-2])
            except Exception:
                pass
            break
    return {'wall_time': t, 'raw': out}


def run_optimized(task_file: str) -> dict:
    proc = subprocess.run([sys.executable, OPT_SCRIPT, '--benchmark', '--file', task_file], capture_output=True, text=True)
    out = proc.stdout + proc.stderr
    # optimized writes benchmark_result.json
    try:
        with open('benchmark_result.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        data = {'raw_output': out}
    return data


if __name__ == '__main__':
    task_file = 'input_new/sample_tasks_small.json'
    print('Running baseline...')
    b = run_baseline(task_file)
    print('Baseline:', b.get('wall_time'), 'sec')
    print('Running optimized...')
    o = run_optimized(task_file)
    print('Optimized:', o.get('wall_time'), 'sec')
    report = {'baseline': b, 'optimized': o}
    with open('benchmark_comparison.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    print('Saved benchmark_comparison.json')
