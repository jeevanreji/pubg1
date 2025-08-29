# -------------------------
# Author: Jeevan Reji (modified)
# Date: 2025-08-28
# -------------------------
"""
Sprint 7 benchmark runner that uses dynamic metadata (via /metadata endpoint).
It follows the same high-level flow as sprint5 but fetches metadata from brokers
instead of reading broker/metadata.json.
"""
import os, time, json, threading, requests
from utils.metrics import Metrics
from utils.load_generator import HttpLoadGenerator
from utils.fault_injector import FaultInjector

HERE = os.path.dirname(__file__)
BOOTSTRAP = ["http://localhost:8000","http://localhost:8001","http://localhost:8002"]

# --- Environment variables (same format as test_sprint5.py) ---
PARTITION = int(os.environ.get("PARTITION", 0))
GROUP_ID = os.environ.get("GROUP_ID", "bench")
TARGET_RPS = float(os.environ.get("TARGET_RPS", 100))
DURATION_SEC = float(os.environ.get("DURATION_SEC", 40))

def get_metadata():
    for b in BOOTSTRAP:
        try:
            r = requests.get(f"{b}/metadata", timeout=1.0)
            r.raise_for_status()
            return r.json()
        except Exception:
            continue
    raise RuntimeError("No brokers available to fetch metadata")

def current_leader():
    md = get_metadata()
    return md["leaders"][str(PARTITION)]

def get_offset(broker_url: str, group_id: str, partition: int) -> int:
    try:
        r = requests.get(f"{broker_url}/offset",
                         params={"group_id": group_id, "partition": partition},
                         timeout=1.0)
        r.raise_for_status()
        return int(r.json().get("offset", 0))
    except Exception:
        return 0

def main():
    metrics = Metrics()

    # Fetch fresh metadata and dump to a local file (so LoadGenerator can read it)
    md = get_metadata()
    metadata_path = os.path.join(HERE, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(md, f)

    # Create load generator
    lg = HttpLoadGenerator(
        metadata_path=metadata_path,
        partition=PARTITION,
        target_rps=TARGET_RPS
    )

    injector = FaultInjector([8000,8001,8002])

    def fault_schedule():
        time.sleep(5)
        print("[FaultSchedule] Killing leader (if any)")
        injector.kill_and_restart(8000, downtime=5)
        time.sleep(5)
        print("[FaultSchedule] Killing a follower (if any)")
        injector.kill_and_restart(8001, downtime=5)

    threading.Thread(target=fault_schedule, daemon=True).start()

    # --- Manual loop to track metrics (like Sprint 5) ---
    t0 = time.time()
    deadline = t0 + DURATION_SEC
    while time.time() < deadline:
        dt = lg.send_once()      # send one message
        metrics.record_pub(dt)   # explicitly record it
        time.sleep(max(0.0, 1.0 / TARGET_RPS))

    s = metrics.summary()
    print(f"Elapsed: {s['elapsed_sec']:.2f}s")
    print(f"Publish: count={s['publish']['count']} errors={s['publish']['errors']} thr={s['publish']['throughput']:.1f}/s")

if __name__ == '__main__':
    main()
