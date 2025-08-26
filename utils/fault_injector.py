import random
import time
import threading

class FaultInjector:
    """
    Simulates random faults in the system such as network delays,
    service crashes, or exceptions.
    """

    def __init__(self, fault_rate=0.1, delay_range=(0.1, 1.0)):
        self.fault_rate = fault_rate
        self.delay_range = delay_range

    def maybe_inject_delay(self):
        """Randomly injects a delay with probability = fault_rate."""
        if random.random() < self.fault_rate:
            delay = random.uniform(*self.delay_range)
            time.sleep(delay)
            return delay
        return 0

    def maybe_raise_exception(self):
        """Randomly raises an exception with probability = fault_rate."""
        if random.random() < self.fault_rate:
            raise RuntimeError("Injected fault: simulated service crash")

    def wrap(self, func):
        """Decorator to wrap functions with fault injection."""
        def wrapped(*args, **kwargs):
            self.maybe_inject_delay()
            self.maybe_raise_exception()
            return func(*args, **kwargs)
        return wrapped
