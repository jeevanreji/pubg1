import requests
import sys
import json

BROKER_URL = "http://localhost:8000"

def produce(key: str, value: str):
    resp = requests.post(f"{BROKER_URL}/publish",
                         json={"key": key, "value": value})
    print("Produced:", resp.json())

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python producer.py <key> <value>")
        sys.exit(1)
    produce(sys.argv[1], sys.argv[2])
