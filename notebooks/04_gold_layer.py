# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler a fact table da silver

# COMMAND ----------

fact_sales = spark.table("workspace.silver.fact_sales")

# COMMAND ----------

display(fact_sales)
fact_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## tabela de Receita diaria para o KPI

# COMMAND ----------

sales_sumary_daily = (
    fact_sales
    .withColumn("order_date", to_date(col("order_purchase_ts")))
    .groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue"),
        round(avg("payment_value"), 2).alias("avg_order_value")
    )

)

# COMMAND ----------

#Salva a tabela
sales_sumary_daily.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.sales_summary_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar tabela de top clientes

# COMMAND ----------

top_customers = (
    fact_sales
    .groupBy("customer_unique_id", "customer_state")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

# COMMAND ----------

#Salvar a tabela
top_customers.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.top_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela de entrada

# COMMAND ----------

delivery_performance = (
    fact_sales
    .groupBy("order_status")
    .agg(
        round(avg("delivery_time_days"), 2).alias("avg_delivery_time_days"),
        countDistinct("order_id").alias("total_orders")
    )
)

# COMMAND ----------

#Salvar a Tabela
delivery_performance.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.delivery_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria tabela de Receita mensal

# COMMAND ----------

monthly_revenue = (
    fact_sales
    .withColumn("year_month", date_format(col("order_purchase_ts"), "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue")
    )
    .orderBy("year_month")
)

# COMMAND ----------

monthly_revenue.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.monthly_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vendas por categoria

# COMMAND ----------

sales_by_category = (
    fact_sales
    .groupBy("product_category_name")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue"),
        round(avg("payment_value"), 2).alias("avg_order_value")
    )
    .orderBy(col("total_revenue").desc())
)

# COMMAND ----------

sales_by_category.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.sales_by_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in workspace.gold;

# COMMAND ----------

display(spark.table("workspace.gold.sales_summary_daily"))
display(spark.table("workspace.gold.sales_by_category"))
display(spark.table("workspace.gold.top_customers"))
display(spark.table("workspace.gold.delivery_performance"))
display(spark.table("workspace.gold.monthly_revenue"))