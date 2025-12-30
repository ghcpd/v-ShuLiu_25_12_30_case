import os
import time
import random
import json
from task_queue_optimized import TaskQueue, Task


def test_retry_and_deadletter(tmp_path):
    tq = TaskQueue(max_workers=2, queue_size=10)
    # isolate idempotency and storage
    tq.idemp = __import__('task_queue_optimized').IdempotencyStore(str(tmp_path / 'db_retry.sqlite'))
    tq.processor.storage_dir = str(tmp_path / 'storage_retry')
    # configure processor to always transient fail
    def always_fail_process(task):
        from task_queue_optimized import TransientError
        raise TransientError('boom')
    tq.processor.process = always_fail_process
    tq.start()
    t = Task(id='r1', image_key='img_fail', max_retries=2)
    tq.enqueue(t)
    tq.stop(wait=True)
    snap = tq.export_metrics()
    assert snap['counters']['retries'] >= 1
    # after exceeding retries, failed should be incremented
    assert snap['counters']['failed'] >= 1
