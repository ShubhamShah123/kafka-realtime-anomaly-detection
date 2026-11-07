import json
from confluent_kafka import Consumer, KafkaError

def main():
    # Configure the consumer with broker address and group settings.
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "basic-group", 
        "auto.offset.reset": "earliest",
    })

    # Subscribe to the sensor-reading topic.
    consumer.subscribe(["sensor-readings"])
    print("[+] Basic consumer started. Reading events ... (Ctrl+C to stop)")

    # Polling loop
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
            # Decoding the json event
            reading = json.loads(msg.value().decode("utf-8"))
            print(f"[+] [PARTITION: {msg.partition()}] "
                  f"sensor={reading['sensor_id']} "
                  f"temp={reading['temperature']} ")
    except KeyboardInterrupt:
        print("[X] Stopping Consumer...")
    finally:
        consumer.close()
    # end try


if __name__ == "__main__":
    main()