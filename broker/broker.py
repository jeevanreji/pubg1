# -------------------------
# Author: Jeevan Reji (modified)
# Date: 2025-08-28
# -------------------------
from fastapi import FastAPI, HTTPException, Request
import json, os, time, hashlib, asyncio
from typing import Dict, List
import requests
from threading import Lock, Thread

app = FastAPI()

# Number of partitions (kept small for tests)
NUM_PARTITIONS = int(os.environ.get("NUM_PARTITIONS", 3))

# Port and base url for this broker
try:
    PORT = int(os.environ["BROKER_PORT"])
except KeyError:
    raise RuntimeError("BROKER_PORT env var must be set (e.g., BROKER_PORT=8000)")
BASE_URL = f"http://localhost:{PORT}"

# Cluster configuration (list of ports). If not provided, use the common 4-node default.
# This expects an env var BROKER_CLUSTER like "8000,8001,8002,8003"
CLUSTER_PORTS = [int(p) for p in os.environ.get("BROKER_CLUSTER", "8000,8001,8002,8003").split(",") if p.strip()!='']
CLUSTER_URLS = [f"http://localhost:{p}" for p in CLUSTER_PORTS]

HERE = os.path.dirname(__file__)
LOG_DIR = f"logs_{PORT}"
os.makedirs(LOG_DIR, exist_ok=True)

# -------------------------
# In-memory state for partitions and offsets (durable to files)
# -------------------------
partitions: List[List[Dict]] = [[] for _ in range(NUM_PARTITIONS)]
locks: List[Lock] = [Lock() for _ in range(NUM_PARTITIONS)]
consumer_offsets: Dict[str, Dict[int, int]] = {}

# raftos-backed replicated stores (if available)
raft_available = False
leaders_store = None
partitions_store = None

async def setup_raft():
    global raft_available, leaders_store, partitions_store
    try:
        import raftos
    except Exception:
        print("[broker] raftos not available; running without raft-backed metadata replication")
        raft_available = False
        return

    raft_available = True
    # configure log path so each broker uses a separate directory
    # (raftos.configure is typically synchronous)
    try:
        raftos.configure({'log_path': f'./raft_logs_{PORT}'})
    except Exception as e:
        print(f"[broker:{PORT}] raftos.configure() warning: {e}")

    # Build cluster node strings (host:port) from CLUSTER_PORTS env,
    # excluding self from the cluster_nodes list passed to register()
    cluster_nodes = [f"127.0.0.1:{p}" for p in CLUSTER_PORTS if p != PORT]

    # Register this node as "host:port" string — raftos expects strings like "127.0.0.1:8000"
    try:
        await raftos.register(f"127.0.0.1:{PORT}", cluster=cluster_nodes)
    except Exception as e:
        # raftos.register may raise if peers not yet available; log and continue.
        print(f"[broker:{PORT}] raftos.register() raised: {e}")

    # create replicated dicts for partitions and leaders
    try:
        leaders_store = raftos.ReplicatedDict('leaders')
        partitions_store = raftos.ReplicatedDict('partitions')
    except Exception as e:
        # Some raftos versions might require different usage; fallback to no-raft mode
        print(f"[broker:{PORT}] Could not create ReplicatedDicts: {e}")
        raft_available = False
        leaders_store = None
        partitions_store = None
        return

    # populate partitions and leaders deterministically based on cluster membership
    replication_factor = min(3, max(1, len(CLUSTER_URLS)))
    for pid in range(NUM_PARTITIONS):
        start = pid % len(CLUSTER_URLS)
        replicas = [CLUSTER_URLS[(start + i) % len(CLUSTER_URLS)] for i in range(replication_factor)]
        # store in replicated dicts (idempotent if multiple nodes run same code)
        try:
            await partitions_store.set(str(pid), replicas)
            await leaders_store.set(str(pid), replicas[0])
        except Exception as e:
            print(f"[broker:{PORT}] warning setting replicated dicts: {e}")

    print(f"[broker:{PORT}] raftos setup complete (or attempted). Cluster members: {CLUSTER_URLS}")

# schedule raft setup on application startup
@app.on_event("startup")
async def _startup_event():
    # run raft setup in the event loop
    await setup_raft()

async def get_metadata() -> Dict:
    """Return current metadata describing partitions and leaders.
    If raft is available use replicated dicts, otherwise compute deterministically
    from CLUSTER_URLS.
    """
    if raft_available and partitions_store is not None and leaders_store is not None:
        parts = {}
        leaders = {}
        for pid in range(NUM_PARTITIONS):
            try:
                p = await partitions_store.get(str(pid))
            except Exception:
                p = None
            try:
                l = await leaders_store.get(str(pid))
            except Exception:
                l = None
            # fallback to deterministic
            if not p:
                start = pid % len(CLUSTER_URLS)
                p = [CLUSTER_URLS[(start + i) % len(CLUSTER_URLS)] for i in range(min(3, len(CLUSTER_URLS)))]
            if not l:
                l = p[0]
            parts[str(pid)] = p
            leaders[str(pid)] = l
        return {"partitions": parts, "leaders": leaders, "members": CLUSTER_URLS}
    else:
        parts = {}
        leaders = {}
        for pid in range(NUM_PARTITIONS):
            start = pid % len(CLUSTER_URLS)
            replicas = [CLUSTER_URLS[(start + i) % len(CLUSTER_URLS)] for i in range(min(3, len(CLUSTER_URLS)))]
            parts[str(pid)] = replicas
            leaders[str(pid)] = replicas[0]
        return {"partitions": parts, "leaders": leaders, "members": CLUSTER_URLS}

def part_file(pid: int) -> str:
    return os.path.join(LOG_DIR, f"partition_{pid}.jsonl")

def append_message(pid: int, msg: Dict) -> int:
    """Append message to local partition log and persist to file. Return offset."""
    with locks[pid]:
        partitions[pid].append(msg)
        offset = len(partitions[pid]) - 1
        # persist
        with open(part_file(pid), "a") as f:
            f.write(json.dumps(msg) + "\n")
        return offset

def _load_logs_from_disk():
    for pid in range(NUM_PARTITIONS):
        fp = part_file(pid)
        if os.path.exists(fp):
            with open(fp, "r") as f:
                lines = [l.strip() for l in f.readlines() if l.strip()]
            for ln in lines:
                try:
                    partitions[pid].append(json.loads(ln))
                except Exception:
                    pass

# load any persisted logs at startup
_load_logs_from_disk()

@app.get("/metadata")
async def metadata_endpoint():
    return await get_metadata()

@app.get("/health")
async def health():
    return {"status": "ok", "port": PORT}

@app.post("/publish")
async def publish(request: Request):
    # Producers POST here. If this broker is not the leader for the partition
    # we return a short response telling the client who the leader is.
    data = await request.json()
    partition = int(data.get("partition"))
    if partition < 0 or partition >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="invalid partition")

    md = await get_metadata()
    leader = md["leaders"][str(partition)]

    # If this broker is not the leader, reply with leader redirect information.
    if leader != BASE_URL:
        return {"status": "redirect", "leader": leader}

    # Append to local log
    offset = append_message(partition, data)

    # replicate to followers (best-effort; follower will persist on /replicate)
    followers = [u for u in md["partitions"][str(partition)] if u != BASE_URL]
    for follower in followers:
        try:
            requests.post(f"{follower}/replicate", json={"partition": partition, "msg": data}, timeout=1.0)
        except Exception as e:
            # replication best-effort: log and continue
            print(f"[broker:{PORT}] replicate to {follower} failed: {e}")

    return {"status": "ok", "offset": offset}

@app.post("/replicate")
async def replicate(request: Request):
    # Followers receive replicated messages from leader. Persist locally.
    body = await request.json()
    partition = int(body.get("partition"))
    msg = body.get("msg")
    if partition < 0 or partition >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="invalid partition")
    offset = append_message(partition, msg)
    return {"status": "ok", "offset": offset}

@app.get("/consume")
async def consume(partition: int, offset: int = 0):
    if partition < 0 or partition >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="invalid partition")
    with locks[partition]:
        msgs = partitions[partition][offset:]
        next_off = len(partitions[partition])
    return {"messages": msgs, "next_offset": next_off}

@app.get("/offset")
async def get_offset(group_id: str, partition: int):
    group = consumer_offsets.get(group_id, {})
    return {"offset": group.get(int(partition), 0)}

@app.post("/commit_offset")
async def commit_offset(request: Request):
    data = await request.json()
    group_id = data.get("group_id")
    partition = int(data.get("partition"))
    offset = int(data.get("offset"))
    consumer_offsets.setdefault(group_id, {})[partition] = offset
    return {"status": "ok"}

@app.get("/loglen")
async def log_length(partition: int):
    if partition < 0 or partition >= NUM_PARTITIONS:
        raise HTTPException(status_code=400, detail="invalid partition")
    return {"length": len(partitions[partition])}
