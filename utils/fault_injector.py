# -------------------------
# Author: Jeevan Reji
# Date: 2025-08-28
# -------------------------
import subprocess
import time
import os
import signal
import psutil   

class FaultInjector:
    """
    Directly kills brokers running on given ports (like Ctrl+C),
    and restarts them using the same commands you’d run in terminal.
    """

    def __init__(self, broker_ports):
        self.broker_ports = broker_ports

    def _find_pid_on_port(self, port):
        """Return PID of process listening on the port, or None."""
        for proc in psutil.process_iter(["pid"]):
            try:
                for conn in proc.connections(kind="inet"):
                    if conn.laddr.port == port:
                        return proc.pid
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
        return None

    def kill_broker(self, port):
        pid = self._find_pid_on_port(port)
        if pid:
            print(f"[FaultInjector] Killing broker {port} (pid={pid})")
            try:
                os.kill(pid, signal.SIGTERM)   # graceful like Ctrl+C
                time.sleep(1)
            except Exception as e:
                print(f"[FaultInjector] Error killing {port}: {e}")
        else:
            print(f"[FaultInjector] Broker {port} not found")

    def restart_broker(self, port):
        print(f"[FaultInjector] Restarting broker {port}")
        env = os.environ.copy()
        env["BROKER_PORT"] = str(port)
        subprocess.Popen(
            ["python", "-m", "broker.run_broker", str(port)],
            env=env
        )
        time.sleep(2)  # let it boot

    def kill_and_restart(self, port, downtime=5):
        self.kill_broker(port)
        time.sleep(downtime)
        self.restart_broker(port)
