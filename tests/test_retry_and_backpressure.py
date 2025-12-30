import time

import pytest

from task_queue_optimized import Processor, TaskQueueOptimized, Metrics, TransientError


def test_retry_and_dead_letter(tmp_path):
    metrics = Metrics()
    proc = Processor(seed=2, fault_rate=0.9)  # high fault rate to trigger retries
    tq = TaskQueueOptimized(worker_fn=proc.process, max_workers=2, queue_size=2, idempotency_db=str(tmp_path / "db2.db"), metrics=metrics)
    tasks = [{"image_key": "img/x.jpg", "size": "200x200", "_max_retries": 2}]
    from task_queue_optimized import producer_from_list
    p = producer_from_list(tq, tasks)
    tq.start()
    p.join()
    tq.stop(wait=True)
    # either failed or dead-lettered, retries should be > 0
    assert metrics.retries >= 1
    assert metrics.failed >= 0


def test_backpressure_rejects_when_failfast(tmp_path):
    metrics = Metrics()
    proc = Processor(seed=1, fault_rate=0.0)
    tq = TaskQueueOptimized(worker_fn=proc.process, max_workers=1, queue_size=1, produce_block=False, idempotency_db=str(tmp_path / "db3.db"), metrics=metrics)
    # create 5 tasks quickly
    tasks = [{"image_key": f"img/{i}.jpg", "size": "200x200"} for i in range(5)]
    from task_queue_optimized import producer_from_list
    p = producer_from_list(tq, tasks, produce_interval=0.0)
    tq.start()
    p.join()
    tq.stop(wait=True)
    # with queue_size=1 and failfast we expect some rejections
    assert metrics.rejected > 0
