from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col

# JDBC configs
mysql_url = "jdbc:mysql://localhost:3306/data_pipeline_db"
mysql_properties = {
    "user": "root",
    "password": "ips_s.roy",
    "driver": "com.mysql.cj.jdbc.Driver"
}
postgres_url = "jdbc:postgresql://localhost:5432/pipeline_db"
postgres_properties = {
    "user": "spark_user",
    "password": "zyz",
    "driver": "org.postgresql.Driver"
}

# JDBC JARs
mysql_jdbc_driver_path = "/mnt/c/Users/Ipsita/Downloads/mysql-connector-j-9.3.0/mysql-connector-j-9.3.0/mysql-connector-j-9.3.0.jar"
postgres_jdbc_driver_path = "/home/ipsitaroy/lirik_internship/postgresql-42.7.3.jar"

# Create Spark session
spark = SparkSession.builder \
    .appName("Transform MySQL to PostgreSQL") \
    .config("spark.jars", mysql_jdbc_driver_path + "," + postgres_jdbc_driver_path) \
    .getOrCreate()

# ---------- Transform functions ----------

def transform_categories(df):
    return df.dropna().dropDuplicates() \
             .withColumn("CategoryName", initcap(col("CategoryName"))) \
             .withColumnRenamed("CategoryID", "Category_ID") \
             .withColumnRenamed("CategoryName", "Category_Name")

def transform_cities(df):
    return df.select("CityName", "Zipcode") \
             .dropna().dropDuplicates() \
             .withColumnRenamed("CityName", "City_Name") \
             .withColumnRenamed("Zipcode", "Zip_Code") \
             .withColumn("City_Name", initcap(col("City_Name"))) \
             .withColumn("Zip_Code", col("Zip_Code").cast("int"))

# ---------- Read from MySQL ----------

categories_df = spark.read.jdbc(url=mysql_url, table="categories", properties=mysql_properties)
cities_df = spark.read.jdbc(url=mysql_url, table="cities", properties=mysql_properties)

# ---------- Apply transformations ----------

categories_transformed = transform_categories(categories_df)
cities_transformed = transform_cities(cities_df)

# ---------- Write to PostgreSQL ----------

categories_transformed.write.jdbc(url=postgres_url, table="categories", properties=postgres_properties, mode="overwrite")
cities_transformed.write.jdbc(url=postgres_url, table="cities", properties=postgres_properties, mode="overwrite")

spark.stop()
print("✅ Transformed & written to PostgreSQL successfully.")
