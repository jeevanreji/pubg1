import requests
import time
import json
import os
from collections import defaultdict, deque
from datetime import datetime, timedelta

METADATA_FILE = "broker/metadata.json"
SNAPSHOT_FILE = "analytics_snapshot.json"
NUM_PARTITIONS = 3
POLL_INTERVAL = 2.0  # seconds
WINDOW_SIZE = 60  # seconds, sliding window

def load_metadata():
    with open(METADATA_FILE, "r") as f:
        return json.load(f)

def get_leader(partition: int, metadata: dict):
    return metadata["leaders"][str(partition)]

def load_snapshot():
    if os.path.exists(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE) as f:
            snapshot = json.load(f)
            counts = defaultdict(int, snapshot.get("counts", {}))
            offsets = snapshot.get("offsets", {str(p): 0 for p in range(NUM_PARTITIONS)})
            return counts, {int(k): v for k, v in offsets.items()}
    else:
        return defaultdict(int), {p: 0 for p in range(NUM_PARTITIONS)}

def save_snapshot(counts, offsets):
    snapshot = {
        "counts": counts,
        "offsets": offsets
    }
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(snapshot, f)

def consume_and_update(counts, offsets, poll_interval=POLL_INTERVAL):
    window_messages = deque()  # (timestamp, key)
    while True:
        metadata = load_metadata()
        for partition in range(NUM_PARTITIONS):
            broker_url = get_leader(partition, metadata)
            offset = offsets.get(partition, 0)
            try:
                resp = requests.get(f"{broker_url}/consume",
                                    params={"partition": partition, "offset": offset}, timeout=2)
                resp.raise_for_status()
                data = resp.json()
                messages = data.get("messages", [])
                for msg in messages:
                    ts = msg.get("timestamp", time.time())
                    key = msg.get("key")
                    window_messages.append((ts, key))
                    counts[key] += 1
                    offset += 1
                offsets[partition] = offset
            except requests.exceptions.RequestException:
                print(f"[Partition {partition}] Leader unreachable at {broker_url}, skipping...")
        
        # Remove old messages outside the window
        cutoff = time.time() - WINDOW_SIZE
        while window_messages and window_messages[0][0] < cutoff:
            _, key = window_messages.popleft()
            counts[key] -= 1
            if counts[key] <= 0:
                del counts[key]

        # Print current windowed counts
        if counts:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Windowed counts: {dict(counts)}")
        
        # Save snapshot every loop
        save_snapshot(counts, offsets)

        time.sleep(poll_interval)

if __name__ == "__main__":
    counts, offsets = load_snapshot()
    consume_and_update(counts, offsets)
