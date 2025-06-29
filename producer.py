import csv
import json
import os
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_topic_map = {
    "employees-topic": "/home/ipsitaroy/lirik_internship/finished/employees.csv",
    "products-topic": "/home/ipsitaroy/lirik_internship/finished/products.csv",
    "sales-topic": "/home/ipsitaroy/lirik_internship/sales.csv"
}

# Iterate through each topic-path 
for topic, path in csv_topic_map.items():
    if not os.path.exists(path):
        print(f" Path does not exist: {path}")
        continue

    records_sent = 0

    if os.path.isdir(path):
        for filename in os.listdir(path):
            if filename.endswith(".csv"):
                full_path = os.path.join(path, filename)
                try:
                    with open(full_path, mode='r') as file:
                        reader = csv.DictReader(file)
                        for row in reader:
                            producer.send(topic, value=row)
                            records_sent += 1
                            if records_sent % 1000 == 0:
                                print(f" Sent {records_sent} records to {topic} from {filename}...")
                except Exception as e:
                    print(f" Error reading {full_path}: {e}")
    else:
        try:
            with open(path, mode='r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    producer.send(topic, value=row)
                    records_sent += 1
                    if records_sent % 1000 == 0:
                        print(f" Sent {records_sent} records to {topic}...")
        except Exception as e:
            print(f" Error reading {path}: {e}")

    print(f"✅ Finished sending {records_sent} records to {topic}")


producer.flush()
print("✅ All available CSV records have been sent to their respective Kafka topics.")
