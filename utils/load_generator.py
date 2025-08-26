
import time, requests, json, threading, random
from typing import Optional

class HttpLoadGenerator:
    """
    Sends messages to the leader of a given partition at ~target_rps.
    Each message carries a client timestamp to measure end-to-end latency.
    """
    def __init__(self, metadata_path: str, partition: int, target_rps: float = 100.0):
        self.metadata_path = metadata_path
        self.partition = partition
        self.target_rps = target_rps
        self.running = False
        self.sent = 0
        self.errors = 0

    def _load_md(self):
        with open(self.metadata_path, "r") as f:
            return json.load(f)

    def leader_url(self) -> str:
        md = self._load_md()
        return md["leaders"][str(self.partition)]

    def send_once(self, key: Optional[str] = None, value: Optional[str] = None) -> float:
        if key is None:
            key = f"user-{random.randint(0, 999)}"
        if value is None:
            value = f"payload-{random.randint(0, 1_000_000)}"
        payload = {
            "key": key,
            "value": value,
            "partition": self.partition,
            "client_ts": time.time()
        }
        leader = self.leader_url()
        t0 = time.time()
        try:
            resp = requests.post(f"{leader}/publish", json=payload, timeout=1.5)
            resp.raise_for_status()
            self.sent += 1
            return time.time() - t0
        except requests.exceptions.RequestException:
            self.errors += 1
            return -1.0

    def run_for(self, duration_sec: float):
        self.running = True
        deadline = time.time() + duration_sec
        interval = 1.0 / self.target_rps if self.target_rps > 0 else 0.0
        while time.time() < deadline and self.running:
            dt = self.send_once()
            # sleep the remainder of the interval (if any)
            if interval > 0:
                to_sleep = max(0.0, interval)
                time.sleep(to_sleep)
        self.running = False
