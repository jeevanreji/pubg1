import requests
import sys
import time

BROKER_URL = "http://localhost:8000"

def consume(partition: int, start_offset: int = 0, poll_interval: float = 2.0):
    offset = start_offset
    while True:
        resp = requests.get(f"{BROKER_URL}/consume",
                            params={"partition": partition, "offset": offset})
        data = resp.json()
        messages = data.get("messages", [])
        for m in messages:
            print(f"[Partition {partition}] offset={offset}: {m}")
            offset += 1
        time.sleep(poll_interval)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <partition> [start_offset]")
        sys.exit(1)
    partition = int(sys.argv[1])
    start_offset = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    consume(partition, start_offset)
