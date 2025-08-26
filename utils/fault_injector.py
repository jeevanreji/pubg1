
import time

class ManualFaultInjector:
    """
    Manual fault injector for local testing without Docker.
    Prints prompts telling you when to stop/start a broker.
    The benchmark measures outage/recovery durations around those actions.
    """
    def __init__(self, pause_at_sec=5, resume_at_sec=15):
        self.pause_at_sec = pause_at_sec
        self.resume_at_sec = resume_at_sec

    def instructions(self):
        return (
            f"At T+{self.pause_at_sec}s: stop the leader broker (Ctrl+C the terminal).\n"
            f"At T+{self.resume_at_sec}s: start it again.\n"
            "The test continues regardless; metrics will reflect the outage."
        )
