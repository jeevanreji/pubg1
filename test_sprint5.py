"""
Sprint 5 benchmark runner (with auto leader + follower fault injection).
"""
import os, time, json, threading, requests
from utils.metrics import Metrics
from utils.load_generator import HttpLoadGenerator
from utils.fault_injector import FaultInjector

HERE = os.path.dirname(__file__)
METADATA = os.path.join(HERE, "broker", "metadata.json")

PARTITION = int(os.environ.get("PARTITION", 0))
GROUP_ID = os.environ.get("GROUP_ID", "bench")
DURATION_SEC = float(os.environ.get("DURATION_SEC", 40))   # longer run by default
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

START_FROM_TAIL = True

def _loglen(broker_url: str, partition: int) -> int:
    try:
        r = requests.get(f"{broker_url}/loglen", params={"partition": partition}, timeout=1.0)
        r.raise_for_status()
        return int(r.json().get("length", 0))
    except requests.exceptions.RequestException:
        return 0

def consumer_worker(stop_evt: threading.Event):
    offset = 0
    while not stop_evt.is_set():
        leader = current_leader()
        if offset == 0:
            offset = get_offset(leader, GROUP_ID, PARTITION)
            if START_FROM_TAIL and offset == 0:
                offset = _loglen(leader, PARTITION)
        try:
            resp = requests.get(f"{leader}/consume",
                                params={"partition": PARTITION, "offset": offset},
                                timeout=1.0)
            resp.raise_for_status()
            msgs = resp.json().get("messages", [])
            for m in msgs:
                client_ts = m.get("client_ts") or m.get("timestamp")
                if client_ts is not None:
                    metrics.record_consume(time.time() - float(client_ts))
                else:
                    metrics.record_consume(-1.0)
                offset += 1
            if msgs:
                commit_offset(leader, GROUP_ID, PARTITION, offset)
        except requests.exceptions.RequestException:
            metrics.record_consume(-1.0)
        time.sleep(0.2)

def main():
    # Load metadata to know leader/follower members
    with open(METADATA, "r") as f:
        md = json.load(f)
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

    # Fault injector: schedule explicit leader + follower crashes
    ports = [int(u.split(":")[-1]) for u in members]
    injector = FaultInjector([8000, 8001, 8002, 8003])

    def fault_schedule():
        time.sleep(10)
        print("[FaultSchedule] Killing leader broker 8000")
        injector.kill_and_restart(8000, downtime=5)

        time.sleep(10)
        print("[FaultSchedule] Killing follower broker 8001")
        injector.kill_and_restart(8001, downtime=5)

        time.sleep(10)
        print("[FaultSchedule] Killing follower broker 8002")
        injector.kill_and_restart(8002, downtime=5)

        time.sleep(10)
        print("[FaultSchedule] Killing follower broker 8003")
        injector.kill_and_restart(8003, downtime=5)


    threading.Thread(target=fault_schedule, daemon=True).start()

    # Start consumer
    stop_evt = threading.Event()
    t_cons = threading.Thread(target=consumer_worker, args=(stop_evt,), daemon=True)
    t_cons.start()

    # Start load generator
    lg = HttpLoadGenerator(METADATA, PARTITION, TARGET_RPS)

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

    print("\nTip: Fault schedule killed leader at ~10s and follower at ~20s, each for 5s downtime.")

if __name__ == "__main__":
    main()
