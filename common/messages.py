from typing import Dict, Any
import time

def create_message(key: str, value: Any) -> Dict[str, Any]:
    """Wraps user input into a structured message."""
    return {
        "key": key,
        "value": value,
        "timestamp": time.time()
    }
