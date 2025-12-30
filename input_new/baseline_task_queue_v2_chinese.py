# -*- coding: utf-8 -*-
import json
import threading
import time
from typing import List, Dict

# 问题版基线（中文）
# - 队列操作使用粗粒度全局锁
# - 使用无界内存列表作为队列（风险：内存飙升）
# - 无幂等、无重试、无指标
# - 通过完整休眠模拟 I/O，缺少抖动（jitter）

TASKS: List[Dict] = []
LOCK = threading.Lock()


def load_tasks(path: str):
    # 读取任务（保持问题行为：将数据转为列表）
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    global TASKS
    TASKS = list(data)


def process_image(task: Dict) -> Dict:
    # 模拟阻塞 I/O（问题行为）
    time.sleep(0.03)
    return {
        'image_key': task.get('image_key'),
        'variant': task.get('size', '200x200'),
        'status': 'ok'
    }


def worker():
    while True:
        # 更坏的问题行为：把“判断是否有任务 + 取任务 + 处理耗时 I/O”都放在锁里
        # 这会导致所有线程在锁上排队，整体几乎串行，吞吐和 P95 延迟显著变差
        with LOCK:
            task = TASKS.pop(0) if TASKS else None
            if task is None:
                break
            _ = process_image(task)  # 故意在锁内执行耗时操作（更坏版本）


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
