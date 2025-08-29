# -------------------------
# Author: Jeevan Reji
# Date: 2025-08-28
# -------------------------
"""
Usage examples:

# provide explicit cluster ports (keeps parity with previous behavior)
BROKER_PORT=8000 python -m broker.run_broker 8000 8000 8001 8002
BROKER_PORT=8001 python -m broker.run_broker 8001 8000 8001 8002
BROKER_PORT=8002 python -m broker.run_broker 8002 8000 8001 8002

# or (if you prefer) omit the first positional arg and rely on BROKER_PORT env:
BROKER_PORT=8000 python -m broker.run_broker 8000 8001 8002
"""
import uvicorn
import sys
import os

def main():
    # If first positional arg exists, use it as this broker's port.
    # Otherwise fall back to BROKER_PORT env or 8000.
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except Exception:
            raise RuntimeError("First argument must be the broker port (e.g. 8000)")
    else:
        port = int(os.environ.get("BROKER_PORT", 8000))

    # CLI style: python -m broker.run_broker 8000 8000 8001 8002
    # gather cluster port args from remaining positional args (if any)
    cli_cluster_ports = []
    if len(sys.argv) > 2:
        cli_cluster_ports = [p for p in sys.argv[2:]]

    # If the CLI provided cluster ports, use them. Otherwise, check an env var.
    if cli_cluster_ports:
        cluster_ports = cli_cluster_ports
    else:
        cluster_ports = os.environ.get("BROKER_CLUSTER", str(port)).split(",")

    # Ensure the cluster list contains this node too (makes broker/broker.py simpler)
    if str(port) not in cluster_ports:
        cluster_ports.insert(0, str(port))

    # Export env vars for the broker module to consume
    os.environ["BROKER_PORT"] = str(port)
    os.environ["BROKER_CLUSTER"] = ",".join(cluster_ports)

    # Run the FastAPI broker app (broker.broker:app)
    uvicorn.run("broker.broker:app", host="0.0.0.0", port=port, reload=False)

if __name__ == "__main__":
    main()
