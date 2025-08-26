
import threading, statistics, time

class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.pub_latencies = []     # time to ack publish
        self.consume_latencies = [] # end-to-end (client_ts -> consume time)
        self.pub_errors = 0
        self.consume_errors = 0
        self.pub_count = 0
        self.consume_count = 0
        self.t0 = time.time()

    def record_pub(self, dt: float):
        with self.lock:
            if dt >= 0:
                self.pub_latencies.append(dt)
                self.pub_count += 1
            else:
                self.pub_errors += 1

    def record_consume(self, dt: float):
        with self.lock:
            if dt >= 0:
                self.consume_latencies.append(dt)
                self.consume_count += 1
            else:
                self.consume_errors += 1

    def _stats(self, xs):
        if not xs:
            return {"avg": None, "p95": None}
        xs_sorted = sorted(xs)
        p95_idx = max(0, int(0.95 * (len(xs_sorted)-1)))
        return {"avg": statistics.mean(xs_sorted), "p95": xs_sorted[p95_idx]}

    def summary(self):
        with self.lock:
            elapsed = time.time() - self.t0
            return {
                "elapsed_sec": elapsed,
                "publish": {
                    "count": self.pub_count,
                    "errors": self.pub_errors,
                    "throughput": self.pub_count / elapsed if elapsed > 0 else 0.0,
                    "latency": self._stats(self.pub_latencies)
                },
                "consume": {
                    "count": self.consume_count,
                    "errors": self.consume_errors,
                    "throughput": self.consume_count / elapsed if elapsed > 0 else 0.0,
                    "latency": self._stats(self.consume_latencies)
                }
            }
