
# -------------------------
# Author: Jeevan Reji
# Date: 2024-06-20
# -------------------------
from fastapi import FastAPI, HTTPException, Request
import json, os, time, hashlib
from typing import Dict, List
import requests
from threading import Lock

app = FastAPI()


NUM_PARTITIONS = 3

try:
    PORT = int(os.environ["BROKER_PORT"])
except KeyError:
    raise RuntimeError("BROKER_PORT env var must be set (e.g., BROKER_PORT=8000)")
BASE_URL = f"http://localhost:{PORT}"

HERE = os.path.dirname(__file__)
METADATA_PATH = os.path.join(HERE, "metadata.json")

LOG_DIR = f"logs_{PORT}"
os.makedirs(LOG_DIR, exist_ok=True)

# -------------------------
# State
# -------------------------

partitions: List[List[Dict]] = [[] for _ in range(NUM_PARTITIONS)]

consumer_offsets: Dict[str, Dict[int, int]] = {}

locks = [Lock() for _ in range(NUM_PARTITIONS)]

# -------------------------
# Helpers
# -------------------------
def load_metadata():
    with open(METADATA_PATH, "r") as f:
        return json.load(f)

def is_leader(partition_id: int) -> bool:
    md = load_metadata()
    leader_url = md["leaders"][str(partition_id)]
    return leader_url == BASE_URL

def followers_for(partition_id: int):
    md = load_metadata()
    members = md["partitions"][str(partition_id)]

    return [u for u in members if u != BASE_URL]

def part_file(pid: int) -> str:
    return os.path.join(LOG_DIR, f"partition_{pid}.jsonl")

for pid in range(NUM_PARTITIONS):
    path = part_file(pid)
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        partitions[pid].append(json.loads(line))
                    except Exception:

                        pass

def get_partition(key: str) -> int:
    if key is None:

        get_partition.counter = (get_partition.counter + 1) % NUM_PARTITIONS
        return get_partition.counter
    h = hashlib.sha256(key.encode()).hexdigest()
    return int(h, 16) % NUM_PARTITIONS
get_partition.counter = -1

def append_message(pid: int, msg: Dict):
    """Append to in-memory and disk (idempotency not enforced)."""
    with locks[pid]:
        partitions[pid].append(msg)
        with open(part_file(pid), "a") as f:
            f.write(json.dumps(msg) + "\n")
        return len(partitions[pid]) - 1  # offset

# -------------------------
# API
# -------------------------

@app.get("/health")
def health():
    return {"status": "ok", "port": PORT}

@app.post("/publish")
async def publish(request: Request):
    """
    Producer entry point (should be called on leader).
    Chooses partition by key if not provided, appends locally,
    and replicates to followers.
    """
    body = await request.json()
    key = body.get("key")
    value = body.get("value")

    pid = body.get("partition")
    if pid is None:
        pid = get_partition(key)
    if pid < 0 or pid >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="invalid partition")


    msg = {
        "key": key,
        "value": value,
        "client_ts": body.get("client_ts"),  
        "timestamp": time.time(),            
        "partition": pid
    }


    if not is_leader(pid):
        raise HTTPException(status_code=409, detail="not leader for partition")


    offset = append_message(pid, msg)


    for follower_url in followers_for(pid):
        try:
            requests.post(f"{follower_url}/replicate", json=msg, timeout=1.5)
        except requests.exceptions.RequestException:

            pass

    return {"status": "ok", "partition": pid, "offset": offset}

@app.post("/replicate")
async def replicate(request: Request):
    """
    Follower append path. Leaders also accept it (idempotent ignoring duplicates is not implemented here).
    """
    msg = await request.json()
    pid = int(msg["partition"])
    if pid < 0 or pid >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="invalid partition")
    offset = append_message(pid, msg)
    return {"status": "ok", "offset": offset}

@app.get("/consume")
def consume(partition: int, offset: int = 0):
    if partition < 0 or partition >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="invalid partition")
    with locks[partition]:
        msgs = partitions[partition][offset:]
        next_off = len(partitions[partition])
    return {"messages": msgs, "next_offset": next_off}

@app.get("/offset")
def get_offset(group_id: str, partition: int):
    off = consumer_offsets.get(group_id, {}).get(partition, 0)
    return {"offset": off}

@app.post("/commit")
async def commit_offset(request: Request):
    data = await request.json()
    group_id = data.get("group_id")
    partition = data.get("partition")
    offset = data.get("offset")
    if group_id is None or partition is None or offset is None:
        raise HTTPException(status_code=400, detail="Missing required fields")
    consumer_offsets.setdefault(group_id, {})[int(partition)] = int(offset)
    return {"status": "ok"}

@app.get("/loglen")
def log_length(partition: int):
    if partition < 0 or partition >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="invalid partition")
    return {"length": len(partitions[partition])}

