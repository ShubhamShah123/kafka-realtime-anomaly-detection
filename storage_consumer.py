import json
import psycopg2
import os

from datetime import datetime, timezone
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "postgres-storage-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    db = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "sensor_pipeline"),
        user=os.getenv("DB_USER", "sensor_user"),
        password=os.environ["DB_PASSWORD"],
    )

    cursor = db.cursor()
    consumer.subscribe(["sensor-readings"])
    print("[+] Storage consumer started, Waiting for readings ...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[-] Kafka Error: {msg.error()}")
                continue
            try:
                reading = json.loads(msg.value().decode("utf-8"))
                event_time = datetime.fromtimestamp(
                    reading['timestamp'],
                    tz=timezone.utc
                )
                # Saving the reading the db
                cursor.execute(
                    """
                    INSERT INTO sensor_readings
                    (sensor_id, temperature, event_time, is_anomaly)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        reading["sensor_id"],
                        reading["temperature"],
                        event_time,
                        reading["is_anomaly"]
                    )
                )
                db.commit()
                consumer.commit(message=msg, asynchronous=False)
                print(f"[+] Saved: ID:{reading['sensor_id']} | Temp: {reading['temperature']}")
            except Exception as e:
                db.rollback()
                print(f"[-] Failed to save the reading: {e}")
            # end try
    except  KeyboardInterrupt:
        print("[-] Stopping storage consumer...")
    finally:
        consumer.close()
        cursor.close()
        db.close()
        print("[-] Storage consumer stopped!")

if __name__ == "__main__":
    main()