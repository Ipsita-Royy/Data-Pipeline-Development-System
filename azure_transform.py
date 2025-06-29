import os
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, when, concat_ws

# Azure Storage details
AZURE_STORAGE_ACCOUNT_NAME = "abcc (for safety reasons)"
AZURE_STORAGE_ACCESS_KEY = "xyzz(changed for safety reasons)"
AZURE_CONTAINER_NAME = "data"

# PostgreSQL connection 
POSTGRES_URL = "jdbc:postgresql://localhost:5432/pipeline_db"
POSTGRES_USER = "spark_user"
POSTGRES_PASSWORD = "xyzz"
POSTGRES_DRIVER = "/home/ipsitaroy/lirik_internship/postgresql-42.7.3.jar"

BLOB_FILES = ["countries.csv", "customers.csv"]

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AzureBlobToPostgres") \
    .config("spark.jars", POSTGRES_DRIVER) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def download_blob(blob_name, local_path):
    conn_str = f"DefaultEndpointsProtocol=https;AccountName={AZURE_STORAGE_ACCOUNT_NAME};AccountKey={AZURE_STORAGE_ACCESS_KEY};EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=blob_name)

    with open(local_path, "wb") as f:
        f.write(blob_client.download_blob().readall())
    print(f" Downloaded {blob_name} to {local_path}")

# Transformations 

def transform_countries(df):
    return df.dropna() \
             .dropDuplicates() \
             .withColumn("CountryCode", upper(col("CountryCode")))

def transform_customers(df):
    return df.dropDuplicates() \
             .filter(col("Address").isNotNull() & (col("Address") != "")) \
             .withColumn("MiddleInitial", when(col("MiddleInitial").isNull(), "X").otherwise(col("MiddleInitial"))) \
             .withColumn("FullName", concat_ws(" ", "FirstName", "MiddleInitial", "LastName")) \
             .drop("FirstName", "MiddleInitial", "LastName") \
             .withColumn("CustomerID", col("CustomerID").cast("int"))

#  Save to PostgreSQL 
def write_to_postgres(df, table_name):
    print(f" Writing data to PostgreSQL table `{table_name}`...")
    df.write \
      .format("jdbc") \
      .option("url", POSTGRES_URL) \
      .option("dbtable", table_name) \
      .option("user", POSTGRES_USER) \
      .option("password", POSTGRES_PASSWORD) \
      .option("driver", "org.postgresql.Driver") \
      .mode("overwrite") \
      .save()

#  Pipeline Runner 

def run_pipeline():
    
    for blob_file in BLOB_FILES:
        download_blob(blob_file, blob_file)

    countries_df = spark.read.option("header", True).csv("countries.csv")
    customers_df = spark.read.option("header", True).csv("customers.csv")

    countries_transformed = transform_countries(countries_df)
    customers_transformed = transform_customers(customers_df)

    write_to_postgres(countries_transformed, "countries_transformed")
    write_to_postgres(customers_transformed, "customers_transformed")

    spark.stop()
    print(" Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
