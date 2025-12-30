import subprocess
import sys
import json
import os
from task_queue_optimized import benchmark


def test_integration_runs_and_writes(tmp_path):
    # Use small sample tasks file
    sample = 'input_new/sample_tasks_small.json'
    # run optimized benchmark function directly
    # ensure idempotency is isolated by using fresh DB path
    import task_queue_optimized as tqmod
    dbpath = str(tmp_path / 'db_integ.sqlite')
    report = tqmod.benchmark(sample, workers=4, queue_size=50, concurrent_producers=5, idempotency_db=dbpath)
    assert report['metrics']['counters']['enqueued'] >= 1
