import os
import tempfile
import json
import time

from task_queue_optimized import TaskQueue, Task, IdempotencyStore


def test_idempotency(tmp_path):
    db_path = str(tmp_path / 'state.db')
    tq = TaskQueue(max_workers=2, queue_size=10)
    tq.idemp = IdempotencyStore(db_path)
    tq.start()

    t1 = Task(id='1', image_key='imgA')
    t2 = Task(id='2', image_key='imgA')  # same image_key -> same idempotency key

    assert tq.enqueue(t1)
    assert tq.enqueue(t2)

    tq.stop(wait=True)
    snap = tq.export_metrics()
    # should have only one success
    assert snap['counters']['success'] == 1


def test_enqueue_reject_on_full(tmp_path):
    tq = TaskQueue(max_workers=1, queue_size=1, block_on_full=False)
    # processor will block until event is set so we can test queue full rejection deterministically
    import threading
    start_ev = threading.Event()
    def blocked_process(task):
        start_ev.wait(timeout=1)
        return {'status': 'ok'}
    tq.processor.process = blocked_process
    tq.idemp = IdempotencyStore(str(tmp_path / 'db2.sqlite'))
    # Do not start the consumer yet so the queue remains occupied
    t1 = Task(id='1', image_key='a')
    t2 = Task(id='2', image_key='b')
    assert tq.enqueue(t1) is True
    # queue full (consumer not started), next should be rejected
    assert tq.enqueue(t2) is False
    # now start the consumer so tasks drain and then stop
    tq.start()
    tq.stop(wait=True)
    snap = tq.export_metrics()
    assert snap['counters']['rejected'] >= 1
