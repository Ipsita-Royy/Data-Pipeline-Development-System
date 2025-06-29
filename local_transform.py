from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, initcap

# Initialize Spark
spark = SparkSession.builder \
    .appName("TransformAndWriteLocalCSVsToPostgreSQL") \
    .config("spark.jars", "/home/ipsitaroy/lirik_internship/postgresql-42.7.3.jar") \
    .getOrCreate()

# PostgreSQL config
postgres_options = {
    "url": "jdbc:postgresql://localhost:5432/pipeline_db",
    "user": "spark_user",
    "password": "xyz",
    "driver": "org.postgresql.Driver"
}

# ========================
# Employee Transformation
# ========================
def transform_employees(df):
    return df.withColumn(
        "FullName",
        initcap(concat_ws(" ", col("FirstName"), col("MiddleName"), col("LastName")))
    ).select("FullName", "BirthDate", "Gender", "DepartmentID", "HireDate")

# ========================
# Product Transformation
# ========================
def transform_products(df):
    return df.drop("isallergic") \
             .withColumn("ProductName", initcap(col("ProductName")))

# ========================
# Sales Transformation
# ========================
def transform_sales(df):
    return df.drop("SalesPersonID", "TransactionNumber", "TotalPrice")

# ========================
# General CSV Writer
# ========================
def write_to_postgres(df, table_name):
    print(f"🚀 Writing transformed data to table `{table_name}`...")
    df.write \
        .format("jdbc") \
        .option("url", postgres_options["url"]) \
        .option("dbtable", table_name) \
        .option("user", postgres_options["user"]) \
        .option("password", postgres_options["password"]) \
        .option("driver", postgres_options["driver"]) \
        .mode("overwrite") \
        .save()

# ========================
# File Processing Pipeline
# ========================
print("🔄 Starting transformation pipeline...")

# Employees
employees_df = spark.read.option("header", False).csv("employees.csv") \
    .toDF("EmployeeID", "FirstName", "MiddleName", "LastName", "BirthDate", "Gender", "DepartmentID", "HireDate")
employees_transformed = transform_employees(employees_df)
write_to_postgres(employees_transformed, "employees")

# Products
products_df = spark.read.option("header", False).csv("products.csv") \
    .toDF("ProductID", "ProductName", "Price", "CategoryID", "Priority", "CreatedDate", "Durability", "isallergic", "Stock")
products_transformed = transform_products(products_df)
write_to_postgres(products_transformed, "products")

# Sales
sales_df = spark.read.option("header", False).csv("sales.csv") \
    .toDF("TransactionID", "ProductID", "ZipCode", "CustomerID", "Quantity", "Discount", "Tax", "TransactionDate", "TransactionNumber")
sales_transformed = transform_sales(sales_df)
write_to_postgres(sales_transformed, "sales")

print("✅ All CSVs transformed and written to PostgreSQL successfully.")
spark.stop()
