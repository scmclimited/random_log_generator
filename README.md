# 🔧 random_log_generator

- Simulating a production environment with log generation and Kafka-compatible streaming.


## 🚀 Usage:

`python generate_logs.py`

## Or in SIM Kafka mode:


### 🧪 Generate Logs (Simulated Stream to File)

```bash
python main.py --mode stream --count 25 --use-kafka-sim --stream-path "./logs/simulated.jsonl"
```

### 📦 Batch Write to JSON File

```bash
python main.py --mode batch --count 100 --use-kafka-sim --stream-path "./logs/batch_output.json"
```

### 🛰️ Real Kafka Mode

```bash
python main.py \
  --mode stream \
  --count 50 \
  --stream-path "./logs/kafka_backup.jsonl" \
  --brokers "localhost:9092" \
  --topic "audit_logs"
```

> 💡 To disable simulation, omit `--use-kafka-sim`

---

## ⚙️ Arguments Reference

| Argument         | Description                              | Example |
|------------------|------------------------------------------|---------|
| `--mode`         | Output mode: `stream`, `batch`, `parquet` | `--mode stream` |
| `--count`        | Number of logs to generate               | `--count 50` |
| `--use-kafka-sim`| Use simulated Kafka file writer          | `--use-kafka-sim` |
| `--stream-path`  | Path to output file                      | `--stream-path "./logs/out.jsonl"` |
| `--brokers`      | Kafka broker list                        | `--brokers "localhost:9092"` |
| `--topic`        | Kafka topic to publish to                | `--topic audit_logs` |

---

## 📁 Output Examples

- NDJSON (.jsonl): Each log is a separate JSON object per line
- JSON (.json): Full list of logs written as a JSON array
- Parquet (optional): Structured binary format (coming soon)

