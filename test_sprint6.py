"""
Sprint 5 benchmark runner (with auto leader + follower fault injection).
Uses dynamic cluster metadata via /metadata (Raft-backed), not a static metadata.json file.
"""
import os
import time
import json
import threading
import requests
from utils.metrics import Metrics
from utils.load_generator import HttpLoadGenerator
from utils.fault_injector import FaultInjector

HERE = os.path.dirname(__file__)

# --- Configuration ---
# Provide bootstrap brokers as comma-separated env var, e.g.:
# BOOTSTRAP_BROKERS="http://localhost:8000,http://localhost:8001"
BOOTSTRAP_BROKERS = os.environ.get(
    "BOOTSTRAP_BROKERS",
    "http://localhost:8000,http://localhost:8001,http://localhost:8002"
).split(",")

PARTITION = int(os.environ.get("PARTITION", 0))
GROUP_ID = os.environ.get("GROUP_ID", "bench")
DURATION_SEC = float(os.environ.get("DURATION_SEC", 40))   # longer run by default
TARGET_RPS = float(os.environ.get("TARGET_RPS", 100))

metrics = Metrics()

START_FROM_TAIL = True

# --- Helpers to get dynamic metadata from the cluster ---

def fetch_metadata(timeout=1.0) -> dict:
    """
    Try each bootstrap broker's /metadata endpoint and return the first valid metadata dict.
    Raises RuntimeError if none are reachable.
    """
    last_exc = None
    for b in BOOTSTRAP_BROKERS:
        try:
            r = requests.get(f"{b}/metadata", timeout=timeout)
            r.raise_for_status()
            md = r.json()
            # basic sanity check
            if isinstance(md, dict) and "leaders" in md and "partitions" in md:
                return md
        except requests.exceptions.RequestException as e:
            last_exc = e
            # try next bootstrap broker
            continue
    raise RuntimeError(f"Could not fetch metadata from bootstrap brokers: {last_exc}")

def current_leader() -> str:
    md = fetch_metadata()
    return md["leaders"][str(PARTITION)]

def current_members() -> list:
    md = fetch_metadata()
    return md["partitions"][str(PARTITION)]

def get_offset(broker_url: str, group_id: str, partition: int) -> int:
    try:
        r = requests.get(
            f"{broker_url}/offset",
            params={"group_id": group_id, "partition": partition},
            timeout=1.0
        )
        r.raise_for_status()
        return int(r.json().get("offset", 0))
    except requests.exceptions.RequestException:
        return 0

def commit_offset(broker_url: str, group_id: str, partition: int, offset: int):
    try:
        requests.post(
            f"{broker_url}/commit",
            json={"group_id": group_id, "partition": partition, "offset": offset},
            timeout=1.0
        )
    except requests.exceptions.RequestException:
        pass

def _loglen(broker_url: str, partition: int) -> int:
    try:
        r = requests.get(f"{broker_url}/loglen", params={"partition": partition}, timeout=1.0)
        r.raise_for_status()
        return int(r.json().get("length", 0))
    except requests.exceptions.RequestException:
        return 0

# --- Consumer worker (polls leader dynamically) ---
def consumer_worker(stop_evt: threading.Event):
    offset = 0
    while not stop_evt.is_set():
        try:
            leader = current_leader()
        except Exception as e:
            # no metadata available
            metrics.record_consume(-1.0)
            time.sleep(0.5)
            continue

        if offset == 0:
            offset = get_offset(leader, GROUP_ID, PARTITION)
            if START_FROM_TAIL and offset == 0:
                offset = _loglen(leader, PARTITION)

        try:
            resp = requests.get(
                f"{leader}/consume",
                params={"partition": PARTITION, "offset": offset},
                timeout=1.0
            )
            resp.raise_for_status()
            msgs = resp.json().get("messages", [])
            for m in msgs:
                client_ts = m.get("client_ts") or m.get("timestamp")
                if client_ts is not None:
                    try:
                        metrics.record_consume(time.time() - float(client_ts))
                    except Exception:
                        metrics.record_consume(-1.0)
                else:
                    metrics.record_consume(-1.0)
                offset += 1
            if msgs:
                commit_offset(leader, GROUP_ID, PARTITION, offset)
        except requests.exceptions.RequestException:
            # leader unreachable; record an error and retry (leader may change)
            metrics.record_consume(-1.0)

        time.sleep(0.2)

# --- Main runner ---
def main():
    # Fetch metadata to know leader/follower members
    try:
        md = fetch_metadata()
    except RuntimeError as e:
        print("[Sprint5] ERROR: cannot fetch cluster metadata:", e)
        return

    members = md["partitions"][str(PARTITION)]
    leader = md["leaders"][str(PARTITION)]
    print(f"[Sprint5] Testing partition {PARTITION} with members: {members}, leader={leader}")

    # Health checks
    for url in members:
        try:
            h = requests.get(f"{url}/health", timeout=0.8)
            print(f"  {url}/health -> {h.status_code}")
        except Exception as e:
            print(f"  {url} not reachable: {e}")

    # Fault injector: derive ports from members (expect http://host:port)
    ports = []
    for u in members:
        try:
            p = int(u.rsplit(":", 1)[1])
            ports.append(p)
        except Exception:
            continue

    # If FaultInjector expects a superset, you can pass the discovered ports
    # (it previously had [8000,8001,8002,8003]). Use discovered ports if available.
    if ports:
        injector = FaultInjector(ports)
    else:
        injector = FaultInjector([8000, 8001, 8002, 8003])

    def fault_schedule():
        # schedule a sequence of kills; we sample ports in order (wraparound)
        if not ports:
            # fallback sequence
            seq = [8000, 8001, 8002, 8003]
        else:
            seq = ports
        # staggered kills
        time.sleep(10)
        print(f"[FaultSchedule] Killing broker {seq[0]}")
        injector.kill_and_restart(seq[0], downtime=5)

        time.sleep(10)
        if len(seq) > 1:
            print(f"[FaultSchedule] Killing broker {seq[1]}")
            injector.kill_and_restart(seq[1], downtime=5)

        time.sleep(10)
        if len(seq) > 2:
            print(f"[FaultSchedule] Killing broker {seq[2]}")
            injector.kill_and_restart(seq[2], downtime=5)

        # optional fourth
        time.sleep(10)
        if len(seq) > 3:
            print(f"[FaultSchedule] Killing broker {seq[3]}")
            injector.kill_and_restart(seq[3], downtime=5)

    threading.Thread(target=fault_schedule, daemon=True).start()

    # Start consumer
    stop_evt = threading.Event()
    t_cons = threading.Thread(target=consumer_worker, args=(stop_evt,), daemon=True)
    t_cons.start()

    # Start load generator (use bootstrap brokers for metadata resolution inside generator).
    # HttpLoadGenerator signature kept as before; pass bootstrap list as string if it expects metadata file.
    # If HttpLoadGenerator expects METADATA file, you can adapt it to accept BOOTSTRAP_BROKERS instead.
    try:
        lg = HttpLoadGenerator(BOOTSTRAP_BROKERS, PARTITION, TARGET_RPS)
    except TypeError:
        # fallback to old constructor that expects a metadata file path - create a temp metadata.json
        tmp_meta = os.path.join(HERE, "tmp_metadata.json")
        with open(tmp_meta, "w") as f:
            json.dump(md, f)
        lg = HttpLoadGenerator(tmp_meta, PARTITION, TARGET_RPS)

    t0 = time.time()
    deadline = t0 + DURATION_SEC
    while time.time() < deadline:
        dt = lg.send_once()
        metrics.record_pub(dt)
        time.sleep(max(0.0, 1.0 / TARGET_RPS))

    stop_evt.set()
    t_cons.join(timeout=2.0)

    # Report
    s = metrics.summary()
    print("\n=== Sprint 5 Summary ===")
    print(f"Elapsed: {s['elapsed_sec']:.2f}s")
    print(f"Publish: count={s['publish']['count']} errors={s['publish']['errors']} "
          f"thr={s['publish']['throughput']:.1f}/s "
          f"lat_avg={None if s['publish']['latency']['avg'] is None else round(s['publish']['latency']['avg']*1000,1)} ms "
          f"p95={None if s['publish']['latency']['p95'] is None else round(s['publish']['latency']['p95']*1000,1)} ms")
    print(f"Consume: count={s['consume']['count']} errors={s['consume']['errors']} "
          f"thr={s['consume']['throughput']:.1f}/s "
          f"e2e_avg={None if s['consume']['latency']['avg'] is None else round(s['consume']['latency']['avg']*1000,1)} ms "
          f"p95={None if s['consume']['latency']['p95'] is None else round(s['consume']['latency']['p95']*1000,1)} ms")

    print("\nTip: Fault schedule killed a sequence of brokers at roughly 10s intervals, each with downtime=5s.")

if __name__ == "__main__":
    main()
