# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver to Gold: Building BI-Ready Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_customers = spark.table(f"{catalog_name}.silver.slv_customers")
df_payments = spark.table(f"{catalog_name}.silver.slv_payments")
df_orders = spark.table(f"{catalog_name}.silver.slv_orders")

df_customers.createOrReplaceTempView("v_customers")
df_payments.createOrReplaceTempView("v_payments")
df_orders.createOrReplaceTempView("v_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Wrangling with Spark SQL:

# COMMAND ----------

display(spark.sql("SELECT * FROM v_customers LIMIT 5"))

# COMMAND ----------

# Ensuring that I am using the Right Catalog
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

display(spark.sql("SELECT * FROM v_orders LIMIT 5"))

# COMMAND ----------

display(spark.sql("SELECT * FROM v_payments LIMIT 5"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Joining on "customer_id" for orders x payments and city, state mapping
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_ordersdata AS
# MAGIC
# MAGIC WITH customers_city_state AS (
# MAGIC     SELECT
# MAGIC         o.customer_id,
# MAGIC         o.order_id,
# MAGIC         o.order_purchase_timestamp,
# MAGIC         c.customer_zip_code_prefix,
# MAGIC         c.customer_city,
# MAGIC         c.customer_state
# MAGIC     FROM v_orders o
# MAGIC     JOIN v_customers c
# MAGIC         ON o.customer_id = c.customer_id
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM customers_city_state cs
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- cs.customer_id,
# MAGIC -- cs.order_id,
# MAGIC -- p.payment_type,
# MAGIC -- p.payment_value,
# MAGIC -- COALESCE(p.payment_sequential, "None More") AS payment_installments,
# MAGIC -- cs.customer_zip_code_prefix,
# MAGIC -- cs.customer_city,
# MAGIC -- cs.customer_state
# MAGIC -- FROM v_payments p
# MAGIC -- JOIN customers_city_state cs
# MAGIC -- ON 
# MAGIC --     p.order_id = cs.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Combines Customer and Order IDs with their City, State, and Payment Details for as many Transactions
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.gld_transaction_stats AS (
# MAGIC SELECT
# MAGIC   o.`customer_id`,
# MAGIC   o.`order_id`,
# MAGIC   c.`customer_city`,
# MAGIC   c.`customer_state`,
# MAGIC   p.`payment_type`,
# MAGIC   p.`payment_value`,
# MAGIC   p.`payment_installments`
# MAGIC FROM
# MAGIC   `ecommerce`.`silver`.`slv_orders` o
# MAGIC     JOIN `ecommerce`.`silver`.`slv_customers` c
# MAGIC       ON o.`customer_id` = c.`customer_id`
# MAGIC     JOIN `ecommerce`.`silver`.`slv_payments` p
# MAGIC       ON o.`order_id` = p.`order_id`
# MAGIC WHERE
# MAGIC   o.`customer_id` IS NOT NULL
# MAGIC   AND o.`order_id` IS NOT NULL
# MAGIC   AND c.`customer_city` IS NOT NULL
# MAGIC   AND c.`customer_state` IS NOT NULL
# MAGIC   AND p.`payment_type` IS NOT NULL
# MAGIC   AND p.`payment_value` IS NOT NULL
# MAGIC   AND p.`payment_installments` IS NOT NULL
# MAGIC   );

# COMMAND ----------

# Checking the Table out!
display(spark.sql("SELECT * FROM gold.gld_transaction_stats LIMIT 10"))

# COMMAND ----------

