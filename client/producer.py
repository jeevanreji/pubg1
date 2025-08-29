# -------------------------
# Author: Jeevan Reji (modified)
# Date: 2025-08-28
# -------------------------
import requests, sys, json, hashlib, time

BOOTSTRAP_BROKERS = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]

def get_metadata():
    """Fetch cluster metadata from any available broker"""
    for b in BOOTSTRAP_BROKERS:
        try:
            r = requests.get(f"{b}/metadata", timeout=1.0)
            r.raise_for_status()
            return r.json()
        except Exception:
            continue
    raise RuntimeError("No available brokers to fetch metadata from")

def produce(key: str, value: str):
    partition = int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3
    md = get_metadata()
    leader_url = md["leaders"][str(partition)]

    payload = {"key": key, "value": value, "partition": partition, "ts": time.time()}
    # Try leader first
    try:
        r = requests.post(f"{leader_url}/publish", json=payload, timeout=1.0)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "ok":
            print("Produced to leader:", leader_url, "offset=", data.get("offset"))
            return True
        if data.get("status") == "redirect":
            # leader told us which node to send to (rare since we targeted leader)
            leader_url = data.get("leader")
    except Exception:
        # try other replicas for this partition
        pass

    # try replicas if leader unreachable
    for url in md["partitions"][str(partition)]:
        try:
            r = requests.post(f"{url}/publish", json=payload, timeout=1.0)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "ok":
                print("Produced to:", url, "offset=", data.get("offset"))
                return True
            if data.get("status") == "redirect":
                leader_url = data.get("leader")
                # try the redirected leader
                continue
        except Exception:
            continue

    print("Failed to produce message: all replicas unreachable")
    return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python producer.py <key> <value>")
        sys.exit(1)
    produce(sys.argv[1], sys.argv[2])
