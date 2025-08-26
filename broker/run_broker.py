
# broker/run_broker.py
import uvicorn
import sys
import os

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
os.environ["BROKER_PORT"] = str(port)

# Run the ASGI app "broker:app" where "broker" is the module under the broker/ dir.
uvicorn.run("broker.broker:app", host="0.0.0.0", port=port, reload=False)
