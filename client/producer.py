import requests, sys, json

import json

BROKER_METADATA_FILE = "broker/metadata.json"
with open(BROKER_METADATA_FILE) as f:
    metadata = json.load(f)

def produce(key: str, value: str):
    # Determine partition
    partition = int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3
    leader_url = metadata["leaders"][str(partition)]

    # Try sending to leader
    try:
        resp = requests.post(f"{leader_url}/publish", json={"key": key, "value": value})
        msg = resp.json()
    except requests.exceptions.RequestException:
        # Leader unreachable → pick next broker in list
        for url in metadata["partitions"][str(partition)]:
            if url == leader_url:
                continue
            try:
                resp = requests.post(f"{url}/publish", json={"key": key, "value": value})
                msg = resp.json()
                print(f"Leader down, sent to follower {url}")
                break
            except:
                continue
        else:
            print("All brokers down for partition", partition)
            return

    # Replicate to followers if this is leader
    for follower_url in metadata["partitions"][str(partition)]:
        if follower_url == leader_url:
            continue
        try:
            requests.post(f"{follower_url}/replicate", json={"key": key, "value": value, "partition": partition})
        except:
            print("Replication to", follower_url, "failed")
    print("Produced:", msg)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python producer.py <key> <value>")
        sys.exit(1)
    import hashlib
    produce(sys.argv[1], sys.argv[2])
