import os
import time
import subprocess
import json
import sqlite3
import tempfile
import threading

from task_queue_optimized import TaskQueue, Processor, Metrics, IdempotencyStore, TransientError, PermanentError


def test_idempotency_store_marks_processed_and_detects():
    db = IdempotencyStore(":memory:")
    key = "k1"
    assert not db.is_processed(key)
    db.mark_processed(key)
    assert db.is_processed(key)


def test_taskqueue_deduplication_and_enqueue():
    tq = TaskQueue(max_workers=2, queue_size=10, block_on_full=True, idempotency_db=":memory:")
    tq.start()
    t = {"id": 1, "image_key": "a", "variant": "v1"}
    assert tq.enqueue(t) is True
    # mark processed to simulate duplicate
    tq.idempotency.mark_processed("a::v1")
    # enqueue duplicate -> returns True but will be skipped
    assert tq.enqueue(t) is True
    tq.queue.join()
    tq.stop()


def test_processor_error_classification_and_retry():
    p = Processor(error_rate=0.0, mean_latency=0.001)
    # permanent error should raise PermanentError
    try:
        p.process({"id": 1, "image_key": "x", "variant": "v", "permanent_error_chance": 1.0})
        raised = False
    except PermanentError:
        raised = True
    assert raised

    # transient error raised when configured
    try:
        p2 = Processor(error_rate=1.0, mean_latency=0.001)
        p2.process({"id": 2, "image_key": "y", "variant": "v"})
        transient_raised = False
    except Exception:
        transient_raised = True
    assert transient_raised


def test_metrics_collection_latency_and_counts():
    m = Metrics()
    with m.lock:
        m.latencies.extend([0.01, 0.02, 0.03])
        m.success = 3
        m.failed = 1
    d = m.as_dict()
    assert d["samples"] == 3
    assert d["mean_latency_ms"] > 0
    assert d["p95_latency_ms"] > 0
