import requests
import sys
import time
import json

METADATA_FILE = "broker/metadata.json"

def load_metadata():
    """Reload the broker metadata each poll"""
    with open(METADATA_FILE, "r") as f:
        return json.load(f)

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
        metadata = load_metadata()
        broker_url = get_leader(partition, metadata)

        if offset == 0:
            offset = get_group_offset(broker_url, group_id, partition)

        try:
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

        except requests.exceptions.RequestException:
            print(f"[Partition {partition}] Leader unreachable at {broker_url}, retrying...")

        time.sleep(poll_interval)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python consumer.py <partition> <group_id>")
        sys.exit(1)

    partition = int(sys.argv[1])
    group_id = sys.argv[2]

    consume(partition, group_id)
