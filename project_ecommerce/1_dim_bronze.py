# Databricks notebook source
# MAGIC %md
# MAGIC Whatever Dimension's data we have, we will Create a Table in the Bronze layer and Ingest that data.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType

import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = "ecommerce"

# Defining the Schema for the Data file - We will Define Distinct Schema for each CSV we are Uploading
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

# COMMAND ----------

# Path Copied from Catalog -> "source_data" -> "raw" -> Description

# General raw data path: ""/Volumes/ecommerce/source_data/raw/*subfolder name*/*csv name*.csv""

raw_data_path = "/Volumes/ecommerce/source_data/raw/source_data/part_one/df_Customers.csv"
df = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(raw_data_path)

# Adding the Metadata columns
df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.current_timestamp())

display(df.limit(5))

# COMMAND ----------

# You Load data from CSV into Spark DataFrame
# From Spark DataFrame, you Write Data to a Table in Databricks (Delta Table)

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_customers") ## Save Table with this Name

## ".option("mergeSchema", "true") -> Schema Evolution: we get 2 new columns? Will Merge them

# Now, under "ecommerce" and then "bronze" in Catalog, will see a new Table -> Select Compute and 'Start and Close'