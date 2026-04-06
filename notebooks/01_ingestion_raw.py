# Databricks notebook source
# MAGIC %md
# MAGIC ## Importar as tabelas

# COMMAND ----------

base_path = "/Volumes/workspace/default/data-engineering-ecommerce/"

orders = spark.read.csv(base_path + "orders.csv", header=True, inferSchema=True)
customers = spark.read.csv(base_path + "customers.csv", header=True, inferSchema=True)
products = spark.read.csv(base_path + "products.csv", header=True, inferSchema=True)
order_items = spark.read.csv(base_path + "order_items.csv", header=True, inferSchema=True)
payments = spark.read.csv(base_path + "payments.csv", header=True, inferSchema=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar leitura

# COMMAND ----------

display(orders)
print("____________ Head ____________")
products.head()
customers.head()
payments.head()
order_items.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria os Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar o RAW

# COMMAND ----------

orders.write.mode("overwrite").saveAsTable("workspace.raw.orders")
customers.write.mode("overwrite").saveAsTable("workspace.raw.customers")
products.write.mode("overwrite").saveAsTable("workspace.raw.products")
order_items.write.mode("overwrite").saveAsTable("workspace.raw.order_items")
payments.write.mode("overwrite").saveAsTable("workspace.raw.payments")
