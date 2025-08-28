# -------------------------
# Author: Jeevan Reji
# Date: 2024-06-20
# -------------------------
import requests
import sys
import time

BOOTSTRAP_BROKERS = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002",
]

def get_metadata():
    """Fetch cluster metadata from any available broker"""
    for broker in BOOTSTRAP_BROKERS:
        try:
            resp = requests.get(f"{broker}/metadata", timeout=2)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException:
            print(f"[Metadata] Broker {broker} unreachable, trying next...")
    raise RuntimeError("All brokers unreachable for metadata")

def get_leader(partition: int, metadata: dict) -> str:
    """Return the current leader URL for a partition"""
    return metadata["leaders"][str(partition)]

def get_group_offset(broker_url: str, group_id: str, partition: int) -> int:
    """Fetch starting offset for a consumer group"""
    try:
        resp = requests.get(
            f"{broker_url}/offset",
            params={"group_id": group_id, "partition": partition},
            timeout=2
        )
        resp.raise_for_status()
        return resp.json().get("offset", 0)
    except requests.exceptions.RequestException:
        print(f"[Partition {partition}] Could not get offset from {broker_url}, starting at 0")
        return 0

def commit_group_offset(broker_url: str, group_id: str, partition: int, offset: int):
    """Commit the latest consumed offset for a consumer group"""
    try:
        requests.post(
            f"{broker_url}/commit",
            json={
                "group_id": group_id,
                "partition": partition,
                "offset": offset
            },
            timeout=2
        )
    except requests.exceptions.RequestException:
        print(f"[Partition {partition}] Could not commit offset {offset} to {broker_url}")

def consume(partition: int, group_id: str, poll_interval: float = 2.0):
    """Continuously consume messages from a partition and commit offsets"""
    offset = 0
    while True:
        try:
            metadata = get_metadata()
            broker_url = get_leader(partition, metadata)

            if offset == 0:
                offset = get_group_offset(broker_url, group_id, partition)

            resp = requests.get(
                f"{broker_url}/consume",
                params={"partition": partition, "offset": offset},
                timeout=2
            )
            resp.raise_for_status()
            data = resp.json()
            messages = data.get("messages", [])
            for m in messages:
                print(f"[Partition {partition}] offset={offset}: {m}")
                offset += 1

            if messages:
                commit_group_offset(broker_url, group_id, partition, offset)

        except Exception as e:
            print(f"[Partition {partition}] Error: {e}. Retrying...")

        time.sleep(poll_interval)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python consumer.py <partition> <group_id>")
        sys.exit(1)

    partition = int(sys.argv[1])
    group_id = sys.argv[2]

    consume(partition, group_id)
