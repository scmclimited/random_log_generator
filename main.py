import argparse
from generate_logs import generate_log_batch
from kafka_producer import get_kafka_producer


def main() -> None:
    parser = argparse.ArgumentParser(description="CLI tool to simulate log generation and streaming.")
    parser.add_argument("--mode", type=str, choices=["stream", "batch", "parquet"], default="stream",
                        help="The output mode for the logs.")
    parser.add_argument("--count", type=int, default=10,
                        help="Number of logs to generate.")
    parser.add_argument("--use-kafka-sim", type=str, choices=["true", "false"], default="true",
                        help="Whether to use the simulated Kafka producer.")
    parser.add_argument("--stream-path", type=str, default="kafka_simulated.jsonl",
                        help="Path for the simulated Kafka output.")
    parser.add_argument("--brokers", type=str, default="localhost:9092",
                        help="Kafka broker address.")
    parser.add_argument("--topic", type=str, default="audit_logs",
                        help="Kafka topic to publish to.")
    
    args: argparse.Namespace = parser.parse_args()

    # Generate logs
    logs = generate_log_batch(batch_size=args.count)

    # Resolve whether to simulate Kafka
    use_sim = args.use_kafka_sim.lower() == "true"
    
    # Get the appropriate producer
    producer = get_kafka_producer(
        use_simulated=use_sim,
        stream_path=args.stream_path,
        brokers=args.brokers,
        topic=args.topic
    )

    # Send logs
    producer.produce_batch(logs)


if __name__ == "__main__":
    main()
