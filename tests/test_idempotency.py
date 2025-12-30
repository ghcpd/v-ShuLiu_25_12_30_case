import json
import os
import sqlite3
import tempfile

from task_queue_optimized import IdempotencyStore, Processor, TaskQueueOptimized, Metrics


def test_idempotency_skips_duplicate(tmp_path):
    db = str(tmp_path / "meta.db")
    idstore = IdempotencyStore(db)
    # reserve first time -> True
    assert idstore.reserve("k1") is True
    # reserve second time -> False (dedup)
    assert idstore.reserve("k1") is False
    idstore.mark_success("k1", "{}");
    # after success reserve should return False
    assert idstore.reserve("k1") is False
    idstore.close()


def test_queue_processes_once(tmp_path, small_tasks_file):
    metrics = Metrics()
    proc = Processor(seed=1, fault_rate=0.0)
    tq = TaskQueueOptimized(worker_fn=proc.process, max_workers=2, queue_size=10, idempotency_db=str(tmp_path / "db.db"), metrics=metrics)
    tasks = [
        {"image_key": "img/a.jpg", "size": "200x200"},
        {"image_key": "img/a.jpg", "size": "200x200"},
    ]
    th = None
    try:
        from task_queue_optimized import producer_from_list
        th = producer_from_list(tq, tasks)
        tq.start()
        th.join()
        tq.stop(wait=True)
        # first enqueued, second dedup skipped -> dequeued == 1
        assert metrics.dequeued == 1
        assert metrics.success == 1
    finally:
        if th and th.is_alive():
            tq.stop()
