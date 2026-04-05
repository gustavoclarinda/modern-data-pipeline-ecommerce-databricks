# Databricks notebook source
# MAGIC %md
# MAGIC ## Importar Funções 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler Bronze

# COMMAND ----------

orders = spark.table("workspace.bronze.orders")
customers = spark.table("workspace.bronze.customers")
products = spark.table("workspace.bronze.products")
orders_items = spark.table("workspace.bronze.orders_items")
payments = spark.table("workspace.bronze.payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza basica

# COMMAND ----------

orders_clean = (
    orders
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
)

# COMMAND ----------

customers_clean = (
    customers.dropDuplicates(["customer_id"])
)

# COMMAND ----------

order_items_clean = (
    orders_items
    .filter(col("order_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("price") > 0)
)

# COMMAND ----------

products_clean = (
    products
    .dropDuplicates(["product_id"])
    
)

# COMMAND ----------

payments_clean = (
    payments
    .filter(col("payment_value") > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join do Fato Vendas

# COMMAND ----------

fact_sales = (
    order_items_clean.alias("oi")
    
    .join(orders_clean.alias("o"), col("oi.order_id") == col("o.order_id"), "inner")
    .join(customers_clean.alias("c"), col("o.customer_id") == col("c.customer_id"), "left")
    .join(products_clean.alias("p"), col("oi.product_id") == col("p.product_id"), "left")
    .join(payments_clean.alias("pay"), col("o.order_id") == col("pay.order_id"), "left")
    
    .select(
        col("o.order_id"),
        col("o.order_purchase_ts"),
        col("o.order_status"),
        
        col("c.customer_id"),
        col("c.customer_unique_id"),
        col("c.customer_state"),
        
        col("oi.product_id"),
        col("p.product_category_name"),
        
        col("oi.price"),
        col("oi.freight_value"),
        
        col("pay.payment_type"),
        col("pay.payment_value"),
        
        col("o.order_delivered_customer_date"),
        col("o.order_estimated_delivery_date")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar as metricas

# COMMAND ----------

fact_sales = (
    fact_sales
    .withColumn("total_item_value", col("price") + col("freight_value"))
    .withColumn(
        "delivery_time_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_ts"))
    )
)

# COMMAND ----------

display(fact_sales)
fact_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar a Silver

# COMMAND ----------

fact_sales.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar a tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in workspace.silver;