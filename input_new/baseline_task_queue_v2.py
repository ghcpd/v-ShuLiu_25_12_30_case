import json
import threading
import time
from typing import List, Dict, Optional

# Baseline with deliberate flaws:
# - Coarse global lock for queue operations
# - Unbounded in-memory list as queue (risk of memory spikes)
# - No idempotency, no retries, no metrics
# - Full sleep to simulate I/O without jitter

TASKS: List[Dict] = []
LOCK = threading.Lock()


def load_tasks(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    global TASKS
    TASKS = list(data)


def process_image(task: Dict) -> Dict:
    # Simulate blocking I/O
    time.sleep(0.03)
    return {
        'image_key': task.get('image_key'),
        'variant': task.get('size', '200x200'),
        'status': 'ok'
    }


def worker():
    while True:
        with LOCK:
            task = TASKS.pop(0) if TASKS else None
            if task is None:
                break
            _ = process_image(task)



def run(num_workers: int = 4):
    threads = []
    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

if __name__ == '__main__':
    load_tasks('sample_tasks_small.json')
    start = time.time()
    run(4)
    print('done in', time.time() - start, 'sec')
