import os
import json
import random
from datetime import datetime, timezone 
from typing import List, Dict, Optional, Union, Literal

import pandas as pd

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

class LogStreamer:
    def __init__(
        self,
        mode: Literal["stream", "batch", "parquet"] = "stream",
        file_name: Optional[str] = None, # Parameter can be str or None
        base_dir: Optional[str] = None,  # Parameter can be str or None
    ) -> None:
        self.mode = mode
        resolved_file_name: str
        # Use datetime.now(timezone.utc) for a timezone-aware UTC datetime
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        """
        Args:
            mode: One of 'stream', 'batch', or 'parquet'.
            file_name: Relative or absolute path to the output file.
            base_dir: Optional base directory to resolve relative paths.
        """
        # Generate a dynamic filename if not provided
        if file_name is None:
            ext = {"stream": "jsonl", "batch": "json", "parquet": "parquet"}[mode]
            resolved_file_name = f"logs_{mode}_{ts}.{ext}"
        else:
            # If file_name was provided, use it
            resolved_file_name = f"{ts}_{file_name}"

        # Resolve to an absolute path if necessary
        if not os.path.isabs(resolved_file_name):
            # Determine the base directory to use for joining
            resolved_base_dir: str
            if base_dir is None:
                # Use the directory of the current file as the default base
                # Be mindful of __file__ in packaged applications
                resolved_base_dir = os.path.dirname(os.path.abspath(__file__))
            else:
                # Use the provided base_dir (which is a string here)
                resolved_base_dir = base_dir
            # Join the resolved base directory (string) with the resolved file name (string)
            # This ensures os.path.join receives string arguments
            self.file_name = os.path.abspath(os.path.join(resolved_base_dir, resolved_file_name))
        else:
            # If the determined filename was already absolute, use it directly
            self.file_name = resolved_file_name

    def write(self, logs: List[Dict[str, Union[str, int]]]) -> None:
        """
        Writes logs to the specified format based on the selected mode.

        Args:
            logs: A list of log entries as dictionaries.
        """
        try:
            if self.mode == "stream":
                self._write_stream(logs)
            elif self.mode == "batch":
                self._write_batch(logs)
            elif self.mode == "parquet":
                self._write_parquet(logs)
            else:
                raise ValueError(f"Unsupported mode: {self.mode}")
        except Exception as e:
            print(f"Error writing logs: {e}")

    def _write_stream(self, logs: List[Dict[str, Union[str, int]]]) -> None:
        with open(self.file_name, "w") as file:
            for log in logs:
                file.write(f"{json.dumps(log)}\n")
        print(f"Streamed {len(logs)} logs to {self.file_name}")

    def _write_batch(self, logs: List[Dict[str, Union[str, int]]]) -> None:
        with open(self.file_name, "w") as f:
            json.dump(logs, f, indent=2)
        print(f"Wrote batch of {len(logs)} logs to {self.file_name}")

    def _write_parquet(self, logs: List[Dict[str, Union[str, int]]]) -> None:
        df = pd.DataFrame(logs)
        df.to_parquet(self.file_name, engine="pyarrow", index=False)
        print(f"Wrote {len(logs)} logs to {self.file_name} as Parquet")

# Function to generate a list of log dictionaries
def generate_log_batch(batch_size: int = 10) -> List[Dict[str, Union[str, int]]]:
    logs: List[Dict[str, Union[str, int]]] = []
    for _ in range(batch_size):
        log = AuditLog()
        log.generate_random_log()
        logs.append(log.to_dict()) # type: ignore
    return logs

# Function to output to stdout logs based on batch mode
def output_logs(
        logs: List[Dict[str, object]], 
        mode: str = "stdout", 
        file_name: str = "logs.json") -> None:
    try:
        if mode == "stdout":
            print(json.dumps(logs, indent=2))
        elif mode == "file":
            with open(file_name, "w") as f:
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

# Function to output logs based on streaming mode
def output_logs_streaming(
    logs: List[Dict[str, Union[str, int]]],
    file_name: str = "logs_stream.jsonl"
) -> None:
    """
    Outputs logs in newline-delimited JSON format (.jsonl or .ndjson),
    ideal for streaming-style ingestion systems.

    Args:
        logs: A list of log entries as dictionaries.
        file_name: The target file to write logs into (default is 'logs_stream.jsonl').

    Writes:
        Each log entry is written as a single line JSON object.
    """
    try:
        with open(file_name, "w") as file:
            for log in logs:
                line = json.dumps(log)
                file.write(f"{line}\n")
        print(f"Successfully wrote {len(logs)} logs to {file_name}")
    except Exception as e:
        print(f"Error writing logs to stream file: {e}")

# Generate a batch of logs and output them to stdout
# Entry point for testing
if __name__ == "__main__":
    """
    Batch and stream modes available for localized testing
    """
    ## Uncomment the next two lines for controlled batching
    # logs = generate_log_batch(batch_size=10) 
    # output_logs(logs, mode="stdout") # type: ignore

    ## Uncomment the next two lines for controlled streaming
    # logs = generate_log_batch(batch_size=100)
    # output_logs_streaming(logs, file_name="logs_stream.ndjson")

    ## Ideally use LogStreamer class to choose your log generation mode
    logs = generate_log_batch(batch_size=120)

    # Streamed log writing
    LogStreamer(mode="stream", file_name="logs_stream.jsonl").write(logs)

    # Batch writing
    LogStreamer(mode="batch", file_name="logs_batch.json").write(logs)

    # Parquet writing
    LogStreamer(mode="parquet", file_name="logs.parquet").write(logs)

