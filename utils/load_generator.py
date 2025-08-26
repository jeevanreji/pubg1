import threading
import time
import random

class LoadGenerator:
    """
    Generates synthetic load for testing.
    Spawns worker threads that call a given target function repeatedly.
    """

    def __init__(self, target_func, num_workers=5, request_rate=1.0):
        self.target_func = target_func
        self.num_workers = num_workers
        self.request_rate = request_rate  # requests per second
        self.threads = []
        self.running = False

    def _worker(self, worker_id):
        while self.running:
            try:
                self.target_func(worker_id=worker_id, timestamp=time.time())
            except Exception as e:
                print(f"[Worker {worker_id}] Exception: {e}")
            time.sleep(1.0 / self.request_rate)

    def start(self):
        self.running = True
        for i in range(self.num_workers):
            t = threading.Thread(target=self._worker, args=(i,))
            t.daemon = True
            self.threads.append(t)
            t.start()

    def stop(self):
        self.running = False
        for t in self.threads:
            t.join(timeout=1.0)
