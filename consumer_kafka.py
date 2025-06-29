import json
from kafka import KafkaConsumer
import psycopg2
from datetime import datetime

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="pipeline_db",
    user="spark_user",
    password="ips_s.roy"
)
cursor = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(
    'employees-topic', 'products-topic', 'sales-topic',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Helper functions
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else None
    except:
        return None

def parse_timestamp(ts_str):
    if not ts_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return None

# Kafka loop
for msg in consumer:
    topic = msg.topic
    data = msg.value

    try:
        if topic == 'employees-topic':
            cursor.execute("""
                INSERT INTO employees (
                    "EmployeeID", "FirstName", "MiddleInitial", "LastName",
                    "BirthDate", "Gender", "CityID", "HireDate"
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("EmployeeID") DO NOTHING
            """, (
                data.get("EmployeeID"),
                data.get("FirstName"),
                data.get("MiddleInitial"),
                data.get("LastName"),
                parse_date(data.get("BirthDate")),
                data.get("Gender"),
                data.get("CityID"),
                parse_date(data.get("HireDate"))
            ))

        elif topic == 'products-topic':
            cursor.execute("""
                INSERT INTO products (
                    "ProductID", "ProductName", "Price", "CategoryID", "Class",
                    "ModifyDate", "Resistant", "IsAllergic", "VitalityDays"
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("ProductID") DO NOTHING
            """, (
                data.get("ProductID"),
                data.get("ProductName"),
                float(data.get("Price")) if data.get("Price") not in [None, ""] else None,
                data.get("CategoryID"),
                data.get("Class"),
                parse_timestamp(data.get("ModifyDate")),
                data.get("Resistant"),
                data.get("IsAllergic"),
                int(float(data['VitalityDays'])) if data.get("VitalityDays") not in [None, ""] else None
            ))

        elif topic == 'sales-topic':
            cursor.execute("""
                INSERT INTO sales (
                    "SalesID", "SalesPersonID", "CustomerID", "ProductID",
                    "Quantity", "Discount", "TotalPrice", "SalesDate", "TransactionNumber"
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("SalesID") DO NOTHING
            """, (
                data.get("SalesID"),
                data.get("SalesPersonID"),
                data.get("CustomerID"),
                data.get("ProductID"),
                int(data.get("Quantity")) if data.get("Quantity") not in [None, ""] else None,
                float(data.get("Discount")) if data.get("Discount") not in [None, ""] else None,
                float(data.get("TotalPrice")) if data.get("TotalPrice") not in [None, ""] else None,
                parse_date(data.get("SalesDate")),
                data.get("TransactionNumber")
            ))

        conn.commit()
        print(f" Inserted into {topic}: {data}")

    except Exception as e:
        conn.rollback()
        print(f" Failed to insert from {topic}: {e}\nData: {data}")
