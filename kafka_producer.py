import json, os
from typing import List, Dict, Union, Optional, Literal, Any

try:
    from kafka import KafkaProducer  # type: ignore
    KAFKA_AVAILABLE = True
except ImportError:
    KafkaProducer = None  # type: ignore
    KAFKA_AVAILABLE = False


class BaseKafkaProducer:
    def produce(self, log: Dict[str, Union[str, int]]) -> bool:
        raise NotImplementedError

    def produce_batch(self, logs: List[Dict[str, Union[str, int]]]) -> int:
        raise NotImplementedError


class KafkaProducerSimulated(BaseKafkaProducer):
    def __init__(self, stream_path: str = "kafka_simulated.jsonl") -> None:
        self.stream_path = stream_path

    def produce(self, log: Dict[str, Union[str, int]]) -> bool:
        try:
            with open(self.stream_path, "a") as f:
                f.write(json.dumps(log) + "\n")
            print(f"[SIMULATED] Sent 1 log to {self.stream_path}")
            return True
        except Exception as e:
            print(f"[SIMULATED] Failed to write log: {e}")
            return False

    def produce_batch(self, logs: List[Dict[str, Union[str, int]]]) -> int:
        count = 0
        try:
            with open(self.stream_path, "a") as f:
                for log in logs:
                    f.write(json.dumps(log) + "\n")
                    count += 1
            print(f"[SIMULATED] Sent {count} logs to {self.stream_path}")
        except Exception as e:
            print(f"[SIMULATED] Failed to write batch: {e}")
        return count


class KafkaProducerReal(BaseKafkaProducer):
    def __init__(self, brokers: str, topic: str) -> None:
        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python is not installed.")
        self.topic: str = topic
        self.producer: KafkaProducer[Any] = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
            linger_ms=5,
        )

    def produce(self, log: Dict[str, Union[str, int]]) -> bool:
        try:
            self.producer.send(self.topic, log)
            self.producer.flush()
            print(f"[REAL] Sent 1 log to topic '{self.topic}'")
            return True
        except Exception as e:
            print(f"[REAL] Failed to send log: {e}")
            return False

    def produce_batch(self, logs: List[Dict[str, Union[str, int]]]) -> int:
        count = 0
        try:
            for log in logs:
                self.producer.send(self.topic, log)
                count += 1
            self.producer.flush()
            print(f"[REAL] Sent {count} logs to topic '{self.topic}'")
        except Exception as e:
            print(f"[REAL] Kafka batch error: {e}")
        return count


def get_kafka_producer(
    use_simulated: bool = False,
    stream_path: str = "kafka_simulated.jsonl",
    brokers: Optional[str] = None,
    topic: Optional[str] = None
) -> BaseKafkaProducer:
    if use_simulated or not KAFKA_AVAILABLE:
        return KafkaProducerSimulated(stream_path=stream_path)
    if brokers is None or topic is None:
        raise ValueError("Brokers and topic must be provided for real Kafka producer.")
    return KafkaProducerReal(brokers=brokers, topic=topic)
