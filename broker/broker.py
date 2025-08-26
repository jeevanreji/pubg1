from fastapi import FastAPI, Request
import json, os, time, hashlib
from typing import Dict, List

app = FastAPI()

NUM_PARTITIONS = 3
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Partition logs: each one has in-memory list + backing file
partitions: List[List[Dict]] = [[] for _ in range(NUM_PARTITIONS)]

# Load from disk if existing
for pid in range(NUM_PARTITIONS):
    path = os.path.join(LOG_DIR, f"partition_{pid}.jsonl")
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f:
                partitions[pid].append(json.loads(line.strip()))

def get_partition(key: str) -> int:
    if key is None:
        # Round-robin fallback
        get_partition.counter = (get_partition.counter + 1) % NUM_PARTITIONS
        return get_partition.counter
    h = hashlib.sha256(key.encode()).hexdigest()
    return int(h, 16) % NUM_PARTITIONS

get_partition.counter = -1

@app.post("/publish")
async def publish(request: Request):
    msg = await request.json()
    key = msg.get("key")
    partition_id = get_partition(key)
    msg["timestamp"] = time.time()
    msg["partition"] = partition_id

    # Append to in-memory + disk
    partitions[partition_id].append(msg)
    path = os.path.join(LOG_DIR, f"partition_{partition_id}.jsonl")
    with open(path, "a") as f:
        f.write(json.dumps(msg) + "\n")

    return {"status": "ok", "partition": partition_id, "offset": len(partitions[partition_id]) - 1}

@app.get("/consume")
async def consume(partition: int, offset: int = 0):
    if partition < 0 or partition >= NUM_PARTITIONS:
        return {"error": "invalid partition"}
    return {
        "messages": partitions[partition][offset:],
        "next_offset": len(partitions[partition])
    }
