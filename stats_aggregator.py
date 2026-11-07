import json
from confluent_kafka import Consumer, KafkaError

def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "stats-aggregator-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["sensor-readings"])

    sensor_stats = {}
    total_count = 0
    print(f"[+] Stats Aggregator Started. Computing statistics...")
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
            temperature = reading["temperature"]
            if sensor_id not in sensor_stats:
                sensor_stats[sensor_id] = {
                    "sum": 0.0,
                    "count": 0,
                    "min": float("inf"),
                    "max": float("-inf")
                }
            stats = sensor_stats[sensor_id]
            stats["sum"] += temperature
            stats["count"] += 1
            stats["min"] = min(stats["min"], temperature)
            stats["max"] = max(stats["max"], temperature)
            total_count += 1
            if total_count % 10 == 0:
                print(f"\n#### Stats Summary (after {total_count} events.) ####")
                for sid, s in sensor_stats.items():
                    avg = s["sum"] / s["count"]
                    print(f"\t{sid}: avg={avg:.2f} min={s['min']:.2f} "
                            f"max={s['max']:.2f} count={s['count']}")

    except KeyboardInterrupt:
        print("[-] Stopping Stats Aggregator...")
    finally:
        consumer.close()
    # end try    

if __name__ == "__main__":
    main()