import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock


class TaskService:
    def __init__(self):
        self.processing_count = 0
        self.lock = Lock()

    @contextmanager
    def activity(self, task_name):
        with self.lock:
            self.processing_count += 1
            print(f"{task_name}进入，当前任务数：{self.processing_count}")

        try:
            yield
        finally:
            with self.lock:
                self.processing_count -= 1
                print(f"{task_name}完成，当前任务数：{self.processing_count}")

    def process(self, task_name, barrier):
        with self.activity(task_name):
            barrier.wait()  # 等所有任务都进入后再开始处理
            time.sleep(1)


task_names = ["任务一", "任务二", "任务三", "任务四", "任务五",
              "任务六", "任务七", "任务八", "任务九", "任务十"]

service = TaskService()
barrier = Barrier(len(task_names))

with ThreadPoolExecutor(max_workers=len(task_names)) as pool:
    tasks = [
        pool.submit(service.process, name, barrier)
        for name in task_names
    ]

    for task in tasks:
        task.result()