
"""
Sprint 5 benchmark runner (no Docker, two local brokers on 8000/8001).

What it does:
- Starts a consumer thread on a partition and consumer group.
- Starts a load generator that publishes to the leader for that partition.
- Measures publish and end-to-end latencies, throughput.
- (Optional) During the run, you can stop/restart the leader to observe outage/recovery.

Usage:
  # Terminal 1
  BROKER_PORT=8000 python -m broker.run_broker 8000

  # Terminal 2
  BROKER_PORT=8001 python -m broker.run_broker 8001

  # Terminal 3 (this test)
  python test_sprint5.py

Environment variables:
  PARTITION      (default 0)
  GROUP_ID       (default "bench")
  DURATION_SEC   (default 20)
  TARGET_RPS     (default 100)
"""
import os, time, json, threading, requests
from utils.metrics import Metrics
from utils.load_generator import HttpLoadGenerator

HERE = os.path.dirname(__file__)
METADATA = os.path.join(HERE, "broker", "metadata.json")

PARTITION = int(os.environ.get("PARTITION", 0))
GROUP_ID = os.environ.get("GROUP_ID", "bench")
DURATION_SEC = float(os.environ.get("DURATION_SEC", 20))
TARGET_RPS = float(os.environ.get("TARGET_RPS", 100))

metrics = Metrics()

def current_leader() -> str:
    with open(METADATA, "r") as f:
        md = json.load(f)
    return md["leaders"][str(PARTITION)]

def get_offset(broker_url: str, group_id: str, partition: int) -> int:
    try:
        r = requests.get(f"{broker_url}/offset",
                         params={"group_id": group_id, "partition": partition},
                         timeout=1.0)
        r.raise_for_status()
        return int(r.json().get("offset", 0))
    except requests.exceptions.RequestException:
        return 0

def commit_offset(broker_url: str, group_id: str, partition: int, offset: int):
    try:
        requests.post(f"{broker_url}/commit",
                      json={"group_id": group_id, "partition": partition, "offset": offset},
                      timeout=1.0)
    except requests.exceptions.RequestException:
        pass


START_FROM_TAIL = True  # start at end of log to avoid re-reading old messages

def _loglen(broker_url: str, partition: int) -> int:
    try:
        r = requests.get(f"{broker_url}/loglen", params={"partition": partition}, timeout=1.0)
        r.raise_for_status()
        return int(r.json().get("length", 0))
    except requests.exceptions.RequestException:
        return 0

def consumer_worker(stop_evt: threading.Event):
    # Always read from current leader (reload metadata every poll)
    offset = 0
    while not stop_evt.is_set():
        leader = current_leader()
        if offset == 0:
            offset = get_offset(leader, GROUP_ID, PARTITION)
            if START_FROM_TAIL and offset == 0:
                # jump to tail
                offset = _loglen(leader, PARTITION)
        try:
            resp = requests.get(f"{leader}/consume",
                                params={"partition": PARTITION, "offset": offset},
                                timeout=1.0)
            resp.raise_for_status()
            data = resp.json()
            msgs = data.get("messages", [])
            for m in msgs:
                # end-to-end latency: now - client_ts (if present)
                client_ts = m.get("client_ts") or m.get("timestamp")
                if client_ts is not None:
                    metrics.record_consume(time.time() - float(client_ts))
                else:
                    metrics.record_consume(-1.0)
                offset += 1
            if msgs:
                commit_offset(leader, GROUP_ID, PARTITION, offset)
        except requests.exceptions.RequestException:
            # Treat as a consume error (likely broker down)
            metrics.record_consume(-1.0)
        time.sleep(0.2)

def main():
    # Health checks
    with open(METADATA, "r") as f:
        md = json.load(f)
    members = md["partitions"][str(PARTITION)]
    print(f"[Sprint5] Testing partition {PARTITION} with members: {members}, leader={md['leaders'][str(PARTITION)]}")

    for url in members:
        try:
            h = requests.get(f"{url}/health", timeout=0.8)
            print(f"  {url}/health -> {h.status_code}")
        except Exception as e:
            print(f"  {url} not reachable: {e}")

    # Start consumer thread
    stop_evt = threading.Event()
    t_cons = threading.Thread(target=consumer_worker, args=(stop_evt,), daemon=True)
    t_cons.start()

    # Start load generator for duration
    lg = HttpLoadGenerator(METADATA, PARTITION, TARGET_RPS)

    t0 = time.time()
    deadline = t0 + DURATION_SEC
    while time.time() < deadline:
        dt = lg.send_once()
        metrics.record_pub(dt)
        time.sleep(max(0.0, 1.0 / TARGET_RPS))

    # Stop consumer
    stop_evt.set()
    t_cons.join(timeout=2.0)

    # Report
    s = metrics.summary()
    print("\n=== Sprint 5 Summary ===")
    print(f"Elapsed: {s['elapsed_sec']:.2f}s")
    print(f"Publish: count={s['publish']['count']} errors={s['publish']['errors']} thr={s['publish']['throughput']:.1f}/s "
          f"lat_avg={None if s['publish']['latency']['avg'] is None else round(s['publish']['latency']['avg']*1000,1)} ms "
          f"p95={None if s['publish']['latency']['p95'] is None else round(s['publish']['latency']['p95']*1000,1)} ms")
    print(f"Consume: count={s['consume']['count']} errors={s['consume']['errors']} thr={s['consume']['throughput']:.1f}/s "
          f"e2e_avg={None if s['consume']['latency']['avg'] is None else round(s['consume']['latency']['avg']*1000,1)} ms "
          f"p95={None if s['consume']['latency']['p95'] is None else round(s['consume']['latency']['p95']*1000,1)} ms")

    print("\nTip: To test fault tolerance, kill the leader broker during the run and restart it. "
          "Errors will spike and consume throughput will dip; once back, metrics should recover.")

if __name__ == "__main__":
    main()
