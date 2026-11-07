import json
import time
import random

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

def create_topic():
	# Connecting to the Kafka broker and creating the topic.
	admin = AdminClient({
		"bootstrap.servers": "localhost:9092"
	})
	topic = NewTopic("sensor-readings",num_partitions=3,replication_factor=1)
	futures = admin.create_topics([topic])

	for topic_name, future in futures.items():
		try:
			future.result()
			print(f"[+] Topic '{topic_name}' created successfully.")
		except Exception as e:
			print(f"[-] Topic '{topic_name}' already exists or error: {e}.")

def generate_sensor_reading():
	# Simulate a random sensor sending the temperature reading.
	sensor_id = f"sensor-{random.randint(1, 5)}"
	#  10% chance of geenerating the random anomaly
	is_anomaly = random.random() < 0.05
	if is_anomaly:
		temperature = random.uniform(150.0, 200.0)
	else:
		temperature = random.uniform(20.0, 30.0)
	return {
		"sensor_id": sensor_id,
		"temperature": round(temperature, 2),
		"timestamp": time.time(),
		"is_anomaly": is_anomaly
	}

def delivery_report(err, msg):
	# Callback that fires once kafka accpets or rejects a message.
	if err is not None:
		print(f"[-] Delivery Failed: {err}")
	else:
		print(f"[+] Sent: sensor={json.loads(msg.value())['sensor_id']} "
			  f"temp={json.loads(msg.value())['temperature']} "
			  f"partition={msg.partition()}")

def main():
	create_topic()
	producer = Producer({
		"bootstrap.servers": "localhost:9092"
	})
	print("[+] Producer started. Streaming the sensor data... (Ctrl + C to stop)")
	total_generated = 0
	try:
		while True:
			reading = generate_sensor_reading()
			total_generated += 1
			# Sending the reading the kafka, key=sensor_id
			producer.produce(
				topic="sensor-readings",
				key=reading['sensor_id'],
				value=json.dumps(reading),
				callback=delivery_report
			)
			if total_generated % 10 == 0:
				print(f"#### TOTAL READINGS: {total_generated} ####")
			producer.poll(0)
			time.sleep(1)
			
	except KeyboardInterrupt:
		print("[X] Stopping Producer...")
	finally:
		producer.flush()

if __name__ == "__main__":
	main()