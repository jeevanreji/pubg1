# broker/run_broker.py
import uvicorn
import sys

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000

uvicorn.run("broker:app", host="0.0.0.0", port=port, reload=True)
