# -------------------------
# Author: Jeevan Reji
# Date: 2024-06-20
# -------------------------
import requests, sys, json, hashlib

BROKER_METADATA_FILE = "broker/metadata.json"
with open(BROKER_METADATA_FILE) as f:
    metadata = json.load(f)

def produce(key: str, value: str):

    partition = int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3
    leader_url = metadata["leaders"][str(partition)]


    try:
        resp = requests.post(f"{leader_url}/publish", json={"key": key, "value": value})
        msg = resp.json()
    except requests.exceptions.RequestException:

        for url in metadata["partitions"][str(partition)]:
            if url == leader_url:
                continue
            try:
                resp = requests.post(f"{url}/publish", json={"key": key, "value": value})
                msg = resp.json()
                print(f"Leader {leader_url} unreachable, sent to {url}")
                break
            except requests.exceptions.RequestException:
                continue
        else:
            print("All brokers down for partition", partition)
            return

    print("Produced:", msg)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python producer.py <key> <value>")
        sys.exit(1)
    produce(sys.argv[1], sys.argv[2])
