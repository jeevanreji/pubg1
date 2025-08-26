# test_sprint4.py
import requests
import time
import random
import string
import threading
import json

NUM_PARTITIONS = 3
BROKER_METADATA_FILE = "broker/metadata.json"
MESSAGES_PER_SECOND = 5
TEST_DURATION = 60  # seconds

def load_metadata():
    with open(BROKER_METADATA_FILE, "r") as f:
        return json.load(f)

def get_leader(partition: int, metadata: dict):
    return metadata["leaders"][str(partition)]

def random_key(length=3):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def random_value(length=6):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def produce_messages(partition: int):
    end_time = time.time() + TEST_DURATION
    while time.time() < end_time:
        metadata = load_metadata()
        broker_url = get_leader(partition, metadata)
        msg = {
            "key": random_key(),
            "value": random_value()
        }
        try:
            resp = requests.post(f"{broker_url}/publish", json=msg, timeout=2)
            resp.raise_for_status()
            data = resp.json()
            print(f"[Partition {partition}] Sent {msg} -> offset {data.get('offset')}")
        except requests.exceptions.RequestException as e:
            print(f"[Partition {partition}] Failed to send {msg}: {e}")
        time.sleep(1 / MESSAGES_PER_SECOND)

if __name__ == "__main__":
    threads = []
    for p in range(NUM_PARTITIONS):
        t = threading.Thread(target=produce_messages, args=(p,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("Test complete. All messages sent.")
