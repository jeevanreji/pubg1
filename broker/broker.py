from fastapi import FastAPI, HTTPException, Request
import json, os, time, hashlib
from typing import Dict, List

app = FastAPI()

# -------------------------
# Configuration
# -------------------------
NUM_PARTITIONS = 3
port = int(os.environ["BROKER_PORT"])  # Set BROKER_PORT environment variable
LOG_DIR = f"logs_{port}"
os.makedirs(LOG_DIR, exist_ok=True)

# -------------------------
# In-memory state
# -------------------------
# Partition logs: in-memory list + backing file
partitions: List[List[Dict]] = [[] for _ in range(NUM_PARTITIONS)]

# Load from disk if existing
for pid in range(NUM_PARTITIONS):
    path = os.path.join(LOG_DIR, f"partition_{pid}.jsonl")
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f:
                partitions[pid].append(json.loads(line.strip()))

# Consumer offsets: {group_id: {partition: offset}}
consumer_offsets: Dict[str, Dict[int, int]] = {}

# -------------------------
# Helper functions
# -------------------------
def get_partition(key: str) -> int:
    """Hash key to partition; fallback to round-robin if key is None"""
    if key is None:
        get_partition.counter = (get_partition.counter + 1) % NUM_PARTITIONS
        return get_partition.counter
    h = hashlib.sha256(key.encode()).hexdigest()
    return int(h, 16) % NUM_PARTITIONS
get_partition.counter = -1

def append_to_partition(partition_id: int, msg: Dict):
    """Append message to memory + disk"""
    partitions[partition_id].append(msg)
    path = os.path.join(LOG_DIR, f"partition_{partition_id}.jsonl")
    with open(path, "a") as f:
        f.write(json.dumps(msg) + "\n")

# -------------------------
# Endpoints
# -------------------------

@app.post("/publish")
async def publish(request: Request):
    msg = await request.json()
    key = msg.get("key")
    partition_id = get_partition(key)
    msg["timestamp"] = time.time()
    msg["partition"] = partition_id

    append_to_partition(partition_id, msg)
    offset = len(partitions[partition_id]) - 1
    return {"status": "ok", "partition": partition_id, "offset": offset}

@app.get("/consume")
async def consume(partition: int, offset: int = 0):
    if partition < 0 or partition >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="Invalid partition")
    messages = partitions[partition][offset:]
    next_offset = len(partitions[partition])
    return {"messages": messages, "next_offset": next_offset}

@app.post("/replicate")
async def replicate(request: Request):
    """Follower receives replicated message from leader"""
    msg = await request.json()
    partition_id = msg["partition"]
    append_to_partition(partition_id, msg)
    return {"status": "ok"}

# -------------------------
# Consumer group endpoints
# -------------------------
@app.get("/offset")
def get_offset(group_id: str, partition: int):
    """Get current offset for a consumer group"""
    offset = consumer_offsets.get(group_id, {}).get(partition, 0)
    return {"offset": offset}

@app.post("/commit")
def commit_offset(request: Request):
    """Commit offset for a consumer group"""
    payload = request.json() if hasattr(request, "json") else {}
    if not payload:
        raise HTTPException(status_code=400, detail="Missing payload")
    group_id = payload.get("group_id")
    partition = payload.get("partition")
    offset = payload.get("offset")
    if group_id is None or partition is None or offset is None:
        raise HTTPException(status_code=400, detail="Missing required fields")
    consumer_offsets.setdefault(group_id, {})[partition] = offset
    return {"status": "ok"}
