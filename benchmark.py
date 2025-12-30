import json
import os
import sys
import time
from task_queue_optimized import run_optimized, load_tasks

def benchmark(num_tasks: int = 100, concurrency: int = 50, maxsize: int = 100, max_workers: int = 4):
    # Generate tasks if needed
    tasks_file = f"input_new/sample_tasks_{num_tasks}.json"
    if not os.path.exists(tasks_file):
        from input_new.generate_tasks import main as gen_main
        gen_main(num_tasks, tasks_file)

    tasks = load_tasks(tasks_file)
    start = time.perf_counter()
    metrics = run_optimized(tasks, maxsize=maxsize, max_workers=max_workers, producer_threads=concurrency)
    elapsed = time.perf_counter() - start

    stats = metrics.get_stats()
    throughput = stats['total_tasks'] / elapsed
    print(f"Tasks: {num_tasks}, Concurrency: {concurrency}")
    print(f"Throughput: {throughput:.2f} tasks/sec")
    print(f"Mean latency: {stats['mean_latency']:.4f} sec")
    print(f"P95 latency: {stats['p95_latency']:.4f} sec")
    print(f"Error rate: {stats['fail_count'] / stats['total_tasks']:.2%}")
    print(f"Max queue depth: {stats['max_queue_depth']}")
    print(f"Total time: {elapsed:.4f} sec")

    # Save to CSV-like
    with open('benchmark_results.json', 'w') as f:
        json.dump({
            'num_tasks': num_tasks,
            'concurrency': concurrency,
            'throughput': throughput,
            'mean_latency': stats['mean_latency'],
            'p95_latency': stats['p95_latency'],
            'error_rate': stats['fail_count'] / stats['total_tasks'],
            'max_queue_depth': stats['max_queue_depth'],
            'total_time': elapsed
        }, f, indent=2)

if __name__ == '__main__':
    import os
    num_tasks = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    benchmark(num_tasks, concurrency)