import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from datetime import datetime

# Spark Session
spark = SparkSession.builder \
    .appName("API to PostgreSQL - Clean Push") \
    .config("spark.jars", "/home/ipsitaroy/lirik_internship/postgresql-42.7.3.jar") \
    .getOrCreate()

# API Fetch
url = "https://dummyjson.com/carts"
response = requests.get(url)
data = response.json()["carts"]

records = []
for entry in data:
    salesid = entry['id']
    salespersonid = entry['userId']
    salesdate = datetime.utcfromtimestamp(entry['timestamp'] / 1000.0) if 'timestamp' in entry else datetime.now()
    transactionnumber = entry.get('transactionNumber', f"TXN{salesid:05d}")

    for product in entry['products']:
        records.append({
            "salesid": salesid,
            "salespersonid": salespersonid,
            "customerid": product['id'],
            "productid": product['id'],
            "quantity": product['quantity'],
            "discount": product.get('discountPercentage', 0.0) / 100.0,
            # "totalprice": product.get('discountedPrice', 0.0),  # REMOVED
            "salesdate": salesdate,
            "transactionnumber": transactionnumber
        })


# Convert to DataFrame
pdf = pd.DataFrame(records)

schema = StructType([
    StructField("salesid", IntegerType(), False),
    StructField("salespersonid", IntegerType(), True),
    StructField("customerid", IntegerType(), True),
    StructField("productid", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("discount", FloatType(), True),
    StructField("salesdate", TimestampType(), True),
    StructField("transactionnumber", StringType(), True)
])

df = spark.createDataFrame(pdf, schema=schema)

df_capitalized = df \
    .withColumnRenamed("salesid", "SalesID") \
    .withColumnRenamed("salespersonid", "SalesPersonID") \
    .withColumnRenamed("customerid", "CustomerID") \
    .withColumnRenamed("productid", "ProductID") \
    .withColumnRenamed("quantity", "Quantity") \
    .withColumnRenamed("discount", "Discount") \
    .withColumnRenamed("salesdate", "SalesDate") \
    .withColumnRenamed("transactionnumber", "TransactionNumber")

# PostgreSQL Config
postgres_options = {
    "url": "jdbc:postgresql://localhost:5432/pipeline_db",
    "user": "spark_user",
    "password": "xyz",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df, table_name):
    print(f" Writing transformed API data to table `{table_name}`...")
    df.write \
        .format("jdbc") \
        .option("url", postgres_options["url"]) \
        .option("dbtable", table_name) \
        .option("user", postgres_options["user"]) \
        .option("password", postgres_options["password"]) \
        .option("driver", postgres_options["driver"]) \
        .mode("overwrite") \
        .save()

write_to_postgres(df_capitalized, "api_cart_products")

print(" Data written to PostgreSQL successfully.")
