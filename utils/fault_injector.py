# utils/fault_injector.py
import subprocess
import threading
import random
import time
import os
import signal

class FaultInjector(threading.Thread):
    """
    Periodically kills and restarts brokers to simulate failures.
    """
    def __init__(self, broker_ports, min_interval=5, max_interval=15):
        super().__init__(daemon=True)
        self.broker_ports = broker_ports
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.processes = {}
        self.running = True

    def run_broker(self, port):
        env = os.environ.copy()
        env["BROKER_PORT"] = str(port)
        proc = subprocess.Popen(
            ["python", "-m", "broker.run_broker", str(port)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.processes[port] = proc
        return proc

    def kill_broker(self, port):
        if port in self.processes and self.processes[port].poll() is None:
            print(f"[FaultInjector] Killing broker {port}")
            self.processes[port].terminate()
            try:
                self.processes[port].wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.processes[port].kill()
        self.processes.pop(port, None)

    def restart_broker(self, port):
        print(f"[FaultInjector] Restarting broker {port}")
        return self.run_broker(port)

    def run(self):
        # start all brokers initially
        for port in self.broker_ports:
            self.run_broker(port)

        while self.running:
            time.sleep(random.randint(self.min_interval, self.max_interval))
            port = random.choice(self.broker_ports)
            self.kill_broker(port)
            time.sleep(random.randint(3, 6))  # downtime
            self.restart_broker(port)

    def stop(self):
        self.running = False
        for port in list(self.processes.keys()):
            self.kill_broker(port)
