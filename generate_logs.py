# import os
import json
import random
from datetime import datetime
from typing import List, Optional, Dict, Union

# Define a class to encapsulate an audit log entry using properties and Pythonic setters/getters
class AuditLog:
    def __init__(self):
        # Initialize all private attributes for the log entry
        self._timestamp: str | None = None
        self._event: str | None = None
        self._user: str | None = None
        self._source: str | None = None
        self._severity: str | None = None
        self._message: str | None = None
        self._exec_time_ms: int | None = None

    # Property for timestamp
    @property
    def timestamp(self) -> str | None:
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value: str) -> None:
        self._timestamp = value

    # Property for event
    @property
    def event(self) -> str | None :
        return self._event

    @event.setter
    def event(self, value: str) -> None:
        self._event = value

    # Property for user
    @property
    def user(self) -> str | None: 
        return self._user 

    @user.setter
    def user(self, value: str) -> None : 
        self._user = value 

    # Property for source
    @property
    def source(self) -> str | None:
        return self._source

    @source.setter
    def source(self, value: str) -> None:
        self._source = value

    # Property for severity
    @property
    def severity(self) -> str | None:
        return self._severity

    @severity.setter
    def severity(self, value: str) -> None:
        self._severity = value

    # Property for message
    @property
    def message(self) -> Optional[str]:
        return self._message

    @message.setter
    def message(self, value: str) -> None:
        self._message = value

    # Property for execution time in milliseconds
    @property
    def exec_time_ms(self) -> int | None:
        return self._exec_time_ms 

    @exec_time_ms.setter
    def exec_time_ms(self, value: int) -> None:
        self._exec_time_ms = value

    # Method to randomly populate the log fields
    def generate_random_log(self) -> None:
        self.timestamp = datetime.utcnow().isoformat() # type: ignore
        self.event = random.choice(["login", "logout", "file_access", "config_change", "auth_fail", "input_submit"])
        self.user = random.choice(["alice", "bob", "charlie", "dana"])
        self.source = random.choice(["web", "mobile", "api", "admin_portal"])
        self.severity = random.choice(["INFO", "WARN", "ERROR", "DEBUG"])
        self.message = f"Simulated {self.event} event for user {self.user} via {self.source}"
        self.exec_time_ms = random.randint(20, 3000)

    # Method to return the log as a dictionary
    def to_dict(self) -> Dict[str, object]:
        return {
            "timestamp": self.timestamp,
            "event": self.event,
            "user": self.user,
            "source": self.source,
            "severity": self.severity,
            "message": self.message,
            "exec_time_ms": self.exec_time_ms
        }

# Function to generate a list of log dictionaries
def generate_log_batch(batch_size: int = 10) -> List[Dict[str, Union[str, int]]]:
    logs: List[Dict[str, Union[str, int]]] = []
    for _ in range(batch_size):
        log = AuditLog()
        log.generate_random_log()
        logs.append(log.to_dict()) # type: ignore
    return logs

# Function to output the logs based on selected mode
def output_logs(logs: List[Dict[str, object]], mode: str = "stdout", file_path: str = "logs.json") -> None:
    try:
        if mode == "stdout":
            print(json.dumps(logs, indent=2))
        elif mode == "file":
            with open(file_path, "w") as f:
                json.dump(logs, f, indent=2)
        elif mode == "spark":
            spark_ready = [json.dumps(log) for log in logs]
            with open("spark_logs.json", "w") as f:
                for line in spark_ready:
                    f.write(f"{line}\n")
        else:
            raise ValueError("Invalid output mode. Choose 'stdout', 'file', or 'spark'.")
    except Exception as e:
        print(f"Failed to output logs due to: {e}")

# Generate a batch of logs and output them to stdout
# Entry point for testing
if __name__ == "__main__":
    logs = generate_log_batch(batch_size=10)
    output_logs(logs, mode="stdout") # type: ignore