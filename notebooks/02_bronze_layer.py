# Databricks notebook source
# MAGIC %md
# MAGIC ## Importar Funções

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler as Tabelas RAW

# COMMAND ----------

orders_raw = spark.table("workspace.raw.orders")
customers_raw = spark.table("workspace.raw.customers")
products_raw = spark.table("workspace.raw.products")
order_items_raw = spark.table("workspace.raw.order_items")
payments_raw = spark.table("workspace.raw.payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar Leitura

# COMMAND ----------

orders_raw.printSchema()
customers_raw.printSchema()
products_raw.printSchema()
order_items_raw.printSchema()
payments_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformar os orders

# COMMAND ----------

orders_bronze = (
    orders_raw
    .withColumnRenamed("order_purchase_timestamp", "order_purchase_ts")
    .withColumn("order_purchase_ts", col("order_purchase_ts").cast("timestamp"))
    .withColumn("order_approved_at", col("order_approved_at").cast("timestamp"))
    .withColumn("order_delivered_carrier_date", col("order_delivered_carrier_date").cast("timestamp"))
    .withColumn("order_delivered_customer_date", col("order_delivered_customer_date").cast("timestamp"))
    .withColumn("order_estimated_delivery_date", col("order_estimated_delivery_date").cast("timestamp"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

# COMMAND ----------

display(orders_bronze)
orders_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformar Customers

# COMMAND ----------

customers_bronze = (
    customers_raw
    .withColumn("ingestion_timestamp", current_timestamp())
)

# COMMAND ----------

display(customers_bronze)
customers_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformar Produtos

# COMMAND ----------

products_bronze = (
    products_raw
    .withColumn("ingestion_timestamp", current_timestamp())
)

# COMMAND ----------

display(products_bronze)
products_bronze.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformar Order Items

# COMMAND ----------

order_items_bronze = (
    order_items_raw
    .withColumn("shipping_limit_date", col("shipping_limit_date").cast("timestamp"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

# COMMAND ----------

display(order_items_bronze)
order_items_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformar payments

# COMMAND ----------

payments_bronze = (
payments_raw
.withColumn("ingestion_timestamp", current_timestamp())
)

# COMMAND ----------

display(payments_bronze)
payments_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar na Bronze em Delta

# COMMAND ----------

orders_bronze.write.mode("overwrite").format("delta").saveAsTable("workspace.bronze.orders")
customers_bronze.write.mode("overwrite").format("delta").saveAsTable("workspace.bronze.customers")
products_bronze.write.mode("overwrite").format("delta").saveAsTable("workspace.bronze.products")
order_items_bronze.write.mode("overwrite").format("delta").saveAsTable("workspace.bronze.order_items")
payments_bronze.write.mode("overwrite").format("delta").saveAsTable("workspace.bronze.payments")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.bronze