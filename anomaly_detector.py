import json
import math

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

def create_alerts_topic():
    admin = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })
    topic = NewTopic(
        "anomaly-alerts",
        num_partitions=1,
        replication_factor=1
    )
    futures = admin.create_topics([topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"[+] Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"[-] Topic '{topic_name}' already exists or error: {e}.")

def main():
    create_alerts_topic()
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "anomaly-detector-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["sensor-readings"])
    alert_producer = Producer({
        "bootstrap.servers": "localhost:9092"
    })
    sensor_stats = {}
    print("[+] Anomaly Detector Started (with dead letter topic). Monitoring sensors...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[-] Consumer Error: {msg.error()}")
                continue
            reading = json.loads(msg.value().decode("utf-8"))
            sensor_id = reading["sensor_id"]
            temperature  = reading["temperature"]

            if sensor_id not in sensor_stats:
                sensor_stats[sensor_id] = {"readings": [], "count": 0}
            stats = sensor_stats[sensor_id]
            stats["readings"].append(temperature)
            stats["count"] += 1

            if stats["count"] >= 5:
                mean = sum(stats["readings"]) / len(stats["readings"])
                variance = sum((x - mean) ** 2 for x in stats["readings"]) / len(stats["readings"])
                std_dev = math.sqrt(variance) if variance > 0 else 0.01
                z_score = abs(temperature - mean) / std_dev

                if z_score > 2.0:
                    print(f"[*] ALERT: {sensor_id} temp={temperature} "
                          f"z_score={z_score:.2f} (mean={mean:.2f}, std={std_dev:.2f})")
                    alert_event = {
                        "sensor_id": sensor_id,
                        "temperature": temperature,
                        "z_score": round(z_score, 2),
                        "mean": round(mean, 2),
                        "std_dev": round(std_dev, 2),
                        "timestamp": reading["timestamp"],
                    }
                    alert_producer.produce(
                        topic="anomaly-alerts",
                        key=sensor_id,
                        value=json.dumps(alert_event)
                    )
                    alert_producer.poll(0)
                else:
                    print(f"[*] OK: {sensor_id} temp={temperature} "
                          f"z_score={z_score:.2f}")
            else:
                print(f"[*] WARMUP: {sensor_id} temp={temperature} "
                      f"(collecting baseline: {stats['count']}/5)")

    except KeyboardInterrupt:
        print(f"[-] Stopping Anomaly Detector...")
    finally:
        alert_producer.flush()
        consumer.close()
    # end try

if __name__ == "__main__":
    main()