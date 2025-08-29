# -------------------------
# Author: Jeevan Reji (modified)
# Date: 2025-08-28
# -------------------------
import requests, sys, time

BOOTSTRAP_BROKERS = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002",
]

def get_metadata():
    for b in BOOTSTRAP_BROKERS:
        try:
            r = requests.get(f"{b}/metadata", timeout=1.0)
            r.raise_for_status()
            return r.json()
        except Exception:
            continue
    raise RuntimeError("No brokers available to fetch metadata from")

def consume(partition: int, group_id: str, poll_interval: float = 0.5):
    md = get_metadata()
    offset = 0
    try:
        # read committed offset if any
        leader_for_partition = md["leaders"][str(partition)]
        r = requests.get(f"{leader_for_partition}/offset", params={"group_id": group_id, "partition": partition}, timeout=1.0)
        if r.ok:
            offset = int(r.json().get("offset", 0))
    except Exception:
        offset = 0

    print(f"Starting consumer for partition {partition} from offset {offset}")
    while True:
        try:
            md = get_metadata()
            leader = md["leaders"][str(partition)]
            r = requests.get(f"{leader}/consume", params={"partition": partition, "offset": offset}, timeout=1.0)
            r.raise_for_status()
            data = r.json()
            msgs = data.get("messages", [])
            next_offset = data.get("next_offset", offset)
            for m in msgs:
                print(f"[consumer] got message: {m}")
            # commit offset
            try:
                requests.post(f"{leader}/commit_offset", json={"group_id": group_id, "partition": partition, "offset": next_offset}, timeout=1.0)
            except Exception:
                pass
            offset = next_offset
        except Exception as e:
            print(f"[consumer] Error: {e}. Retrying...")
        time.sleep(poll_interval)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python consumer.py <partition> <group_id>")
        sys.exit(1)
    partition = int(sys.argv[1])
    group_id = sys.argv[2]
    consume(partition, group_id)
