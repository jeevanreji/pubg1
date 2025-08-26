from fastapi import FastAPI, Request
import json, os, time

app = FastAPI()

LOG_FILE = "log.jsonl"
log = []

# Load existing log from disk if available
if os.path.exists(LOG_FILE):
    with open(LOG_FILE, "r") as f:
        for line in f:
            log.append(json.loads(line.strip()))

@app.post("/publish")
async def publish(request: Request):
    msg = await request.json()
    msg["timestamp"] = time.time()
    log.append(msg)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(msg) + "\n")
    return {"status": "ok", "offset": len(log) - 1}

@app.get("/consume")
async def consume(offset: int = 0):
    return {
        "messages": log[offset:],
        "next_offset": len(log)
    }
