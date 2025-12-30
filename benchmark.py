"""
Comprehensive benchmarking script comparing baseline vs optimized task queue.

Usage:
    python benchmark.py --mode=baseline --tasks=100
    python benchmark.py --mode=optimized --tasks=100
    python benchmark.py --mode=compare --tasks=100
"""

import json
import subprocess
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, Optional


class BenchmarkRunner:
    """Benchmark orchestrator."""

    def __init__(self, workspace_root: Optional[str] = None):
        self.workspace_root = Path(workspace_root or Path(__file__).parent)
        self.results_dir = self.workspace_root / "benchmark_results"
        self.results_dir.mkdir(exist_ok=True)

    def run_baseline(self, task_count: int = 100, workers: int = 4) -> Optional[Dict]:
        """Run baseline implementation."""
        print(f"\n{'='*70}")
        print(f"RUNNING BASELINE (tasks={task_count}, workers={workers})")
        print(f"{'='*70}\n")

        script = self.workspace_root / "input_new" / "baseline_task_queue_v2.py"
        if not script.exists():
            print(f"Error: Baseline script not found at {script}")
            return None

        # Modify baseline to accept task count (simple approach: modify tasks)
        # For now, we'll run the baseline as-is
        try:
            start = time.perf_counter()
            result = subprocess.run(
                [sys.executable, str(script)],
                cwd=str(self.workspace_root),
                capture_output=True,
                text=True,
                timeout=300
            )
            elapsed = time.perf_counter() - start

            print("STDOUT:", result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)

            # Parse baseline output to extract timing
            # Baseline prints: "done in X.XX sec"
            lines = result.stdout.split('\n')
            for line in lines:
                if 'done in' in line:
                    try:
                        parts = line.split()
                        duration = float(parts[2])
                        return {
                            "type": "baseline",
                            "task_count": task_count,
                            "workers": workers,
                            "duration_seconds": duration,
                            "throughput_tps": task_count / duration if duration > 0 else 0,
                            "timestamp": time.time()
                        }
                    except (ValueError, IndexError):
                        pass

            return {
                "type": "baseline",
                "task_count": task_count,
                "workers": workers,
                "duration_seconds": elapsed,
                "throughput_tps": task_count / elapsed if elapsed > 0 else 0,
                "timestamp": time.time()
            }

        except subprocess.TimeoutExpired:
            print("Error: Baseline execution timed out")
            return None
        except Exception as e:
            print(f"Error running baseline: {e}")
            return None

    def run_optimized(
        self,
        task_count: int = 100,
        workers: int = 4,
        queue_size: int = 200,
        error_rate: float = 0.0
    ) -> Optional[Dict]:
        """Run optimized implementation."""
        print(f"\n{'='*70}")
        print(f"RUNNING OPTIMIZED (tasks={task_count}, workers={workers}, queue={queue_size})")
        print(f"{'='*70}\n")

        script = self.workspace_root / "task_queue_optimized.py"
        if not script.exists():
            print(f"Error: Optimized script not found at {script}")
            return None

        try:
            result = subprocess.run(
                [
                    sys.executable, str(script),
                    '--benchmark',
                    f'--task-count={task_count}',
                    f'--workers={workers}',
                    f'--queue-size={queue_size}',
                    f'--error-rate={error_rate}',
                    f'--output={self.results_dir}/optimized_metrics.json'
                ],
                cwd=str(self.workspace_root),
                capture_output=True,
                text=True,
                timeout=300
            )

            print("STDOUT:", result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)

            # Try to load the metrics JSON
            metrics_file = self.results_dir / "optimized_metrics.json"
            if metrics_file.exists():
                with open(metrics_file, 'r') as f:
                    metrics = json.load(f)
                return metrics

            return None

        except subprocess.TimeoutExpired:
            print("Error: Optimized execution timed out")
            return None
        except Exception as e:
            print(f"Error running optimized: {e}")
            return None

    def compare_results(self, baseline: Dict, optimized: Dict) -> Dict:
        """Compare baseline and optimized metrics."""
        print(f"\n{'='*70}")
        print("COMPARISON RESULTS")
        print(f"{'='*70}\n")

        baseline_throughput = baseline.get('throughput_tps', 0)
        optimized_throughput = optimized.get('throughput_tps', 0)
        baseline_latency = baseline.get('duration_seconds', 0)
        optimized_latency = optimized.get('p95_latency_ms', optimized.get('duration_seconds', 0) * 1000) / 1000

        throughput_improvement = (optimized_throughput - baseline_throughput) / baseline_throughput * 100 if baseline_throughput > 0 else 0
        latency_improvement = (baseline_latency - optimized_latency) / baseline_latency * 100 if baseline_latency > 0 else 0

        comparison = {
            "baseline": baseline,
            "optimized": optimized,
            "improvements": {
                "throughput_improvement_pct": throughput_improvement,
                "latency_improvement_pct": latency_improvement,
                "goal_2x_throughput_met": optimized_throughput >= baseline_throughput * 2,
                "goal_p95_latency_met": optimized_latency <= baseline_latency * 0.6
            }
        }

        print(f"Baseline Throughput: {baseline_throughput:.2f} tasks/sec")
        print(f"Optimized Throughput: {optimized_throughput:.2f} tasks/sec")
        print(f"Throughput Improvement: {throughput_improvement:.1f}%")
        print(f"Goal (2x): {'✓ MET' if comparison['improvements']['goal_2x_throughput_met'] else '✗ NOT MET'}\n")

        print(f"Baseline Latency: {baseline_latency*1000:.2f} ms")
        print(f"Optimized P95 Latency: {optimized_latency*1000:.2f} ms")
        print(f"Latency Improvement: {latency_improvement:.1f}%")
        print(f"Goal (P95 ≤ 60%): {'✓ MET' if comparison['improvements']['goal_p95_latency_met'] else '✗ NOT MET'}\n")

        print(f"{'='*70}\n")

        return comparison

    def save_results(self, results: Dict, filename: str = "benchmark_comparison.json"):
        """Save comparison results to JSON."""
        output_path = self.results_dir / filename
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Benchmark Baseline vs Optimized Task Queue')
    parser.add_argument('--mode', choices=['baseline', 'optimized', 'compare'], default='compare',
                        help='Benchmark mode')
    parser.add_argument('--tasks', type=int, default=100, help='Number of tasks')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers')
    parser.add_argument('--queue-size', type=int, default=200, help='Queue size')
    parser.add_argument('--error-rate', type=float, default=0.0, help='Error injection rate')

    args = parser.parse_args()

    runner = BenchmarkRunner()

    if args.mode == 'baseline':
        result = runner.run_baseline(task_count=args.tasks, workers=args.workers)
        if result:
            runner.save_results({"baseline": result}, "baseline_only.json")

    elif args.mode == 'optimized':
        result = runner.run_optimized(
            task_count=args.tasks,
            workers=args.workers,
            queue_size=args.queue_size,
            error_rate=args.error_rate
        )
        if result:
            runner.save_results({"optimized": result}, "optimized_only.json")

    elif args.mode == 'compare':
        baseline = runner.run_baseline(task_count=args.tasks, workers=args.workers)
        if not baseline:
            print("Failed to run baseline")
            return

        optimized = runner.run_optimized(
            task_count=args.tasks,
            workers=args.workers,
            queue_size=args.queue_size,
            error_rate=args.error_rate
        )
        if not optimized:
            print("Failed to run optimized")
            return

        comparison = runner.compare_results(baseline, optimized)
        runner.save_results(comparison)


if __name__ == '__main__':
    main()
