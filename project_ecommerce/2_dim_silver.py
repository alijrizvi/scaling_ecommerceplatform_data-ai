# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

catalog_name = "ecommerce"

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_customers")
df_bronze.show(10)

# COMMAND ----------

# If there are any non-Alphanumeric Characters, Remove them/Replace them with ''s

cols_to_clean = [
    "customer_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
]

for col in cols_to_clean:
    df_bronze = df_bronze.withColumn(
        col,
        F.regexp_replace(
            F.col(col),
            r"[^A-Za-z0-9]",
            ""
        )
    )

df_silver = df_bronze

display(df_silver.show(8))

# COMMAND ----------

# Count of all Unique Cities covered in this Dataset
df_silver.select("customer_city").distinct().count()

# COMMAND ----------

# Removing Duplicate Rows
df_silver = df_silver.dropDuplicates()

# Converting all Customer ID Values into Uppercase
df_silver = df_silver.withColumn(
    "customer_id",
    F.upper(F.col("customer_id")))

display(df_silver)                 

# COMMAND ----------

print("Amount of Customer ID values which are Null:",
      df_silver.filter(F.col("customer_id").isNull()).count()) # None, thankfully!

# COMMAND ----------

# Saving this Table in the Silver Layer (Catalog: ecommerce, Schema: silver, Table: slv_customers)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers") ## Save Table with this Name

# COMMAND ----------

# MAGIC %md
# MAGIC ### So on for the other Tables/Dataframes as well..

# COMMAND ----------

# MAGIC %md
# MAGIC 1:

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType

import pyspark.sql.functions as F

catalog_name = "ecommerce"

# Defining the Schema for the Data file - We will Define Distinct Schema for each CSV we are Uploading
orderitems_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("shipping_charges", FloatType(), True)
])

raw_data_path2 = "/Volumes/ecommerce/source_data/raw/source_data/part_one/df_OrderItems.csv"
df2 = spark.read.option("header", "true").option("delimiter", ",").schema(orderitems_schema).csv(raw_data_path2)

# Adding the Metadata columns
df2 = df2.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.current_timestamp())

# COMMAND ----------

# display(df2.show(7))

# Saving this Table in the Silver Layer too
df2.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_orderitems")

# COMMAND ----------

# MAGIC %md
# MAGIC 2:

# COMMAND ----------

# Defining the Schema for the Data file - We will Define Distinct Schema for each CSV we are Uploading
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True)
])

raw_data_path3 = "/Volumes/ecommerce/source_data/raw/source_data/part_one/df_Orders.csv"
df3 = spark.read.option("header", "true").option("delimiter", ",").schema(orders_schema).csv(raw_data_path3)

# Adding the Metadata columns
df3 = df3.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.current_timestamp())

display(df3.show(6))

# COMMAND ----------

# Saving this (3rd) Table in the Silver Layer too
df3.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC 3:

# COMMAND ----------

# Defining the Schema for the Data file - We will Define Distinct Schema for each CSV we are Uploading
payments_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", FloatType(), True)
])

raw_data_path4 = "/Volumes/ecommerce/source_data/raw/source_data/part_one/df_Payments.csv"
df4 = spark.read.option("header", "true").option("delimiter", ",").schema(payments_schema).csv(raw_data_path4)

# Adding the Metadata columns
df4 = df4.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.current_timestamp())

display(df4.show(6))

# COMMAND ----------

# Saving this (4th) Table in the Silver Layer too
df4.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC 4:

# COMMAND ----------

# Defining the Schema for the Data file - We will Define Distinct Schema for each CSV we are Uploading
products_schema = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("avg_product_weight_grams", FloatType(), True),
    StructField("avg_product_height_cm", FloatType(), True),
    StructField("avg_product_width_cm", FloatType(), True),
    StructField("count_products", IntegerType(), True),
    StructField("avg_product_dimensions", StringType(), True),
    StructField("recency_product_id", StringType(), True)
])

raw_data_path5 = "/Volumes/ecommerce/source_data/raw/source_data/part_one/df_Payments.csv"
df5 = spark.read.option("header", "true").option("delimiter", ",").schema(products_schema).csv(raw_data_path5)

# Adding the Metadata columns
df5 = df5.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.current_timestamp())

display(df5)

# COMMAND ----------

# Saving this (5th) Table in the Silver Layer too
df5.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")