import threading
from task_queue_optimized import TaskQueue, Task


def test_metrics_concurrency(tmp_path):
    tq = TaskQueue(max_workers=4, queue_size=50)
    tq.idemp = __import__('task_queue_optimized').IdempotencyStore(str(tmp_path / 'db_metrics.sqlite'))
    tq.processor.storage_dir = str(tmp_path / 'storage_metrics')
    tq.start()
    total = 100
    def prod(start, end):
        for i in range(start, end):
            tq.enqueue(Task(id=str(i), image_key=f'img_{i}'))

    threads = []
    for s in range(4):
        th = threading.Thread(target=prod, args=(s*(total//4), (s+1)*(total//4)))
        th.start()
        threads.append(th)
    for th in threads:
        th.join()
    tq.stop(wait=True)
    snap = tq.export_metrics()
    # All tasks should be processed
    assert snap['counters']['success'] + snap['counters']['failed'] == total
    assert snap['counters']['enqueued'] == total
