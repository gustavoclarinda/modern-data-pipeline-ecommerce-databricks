# Modern Data Pipeline for E-commerce Analytics using Databricks

## 📌 Overview
This project demonstrates an end-to-end data engineering pipeline using Databricks and the Medallion Architecture (Bronze, Silver, Gold).

The pipeline ingests raw e-commerce data, processes it through multiple transformation layers, and delivers business-ready insights through a dashboard.

---

## 🧱 Architecture

Medallion Architecture:

RAW → BRONZE → SILVER → GOLD

- **Raw**: Ingested CSV files from source
- **Bronze**: Cleaned and standardized data
- **Silver**: Joined and enriched datasets
- **Gold**: Aggregated business metrics for analytics

---

## 📊 Data Source

Brazilian E-commerce Public Dataset (Olist)

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

## ⚙️ Technologies Used

- Databricks
- PySpark
- Delta Lake
- SQL
- Databricks Jobs (orchestration)

---

## 🔄 Pipeline Flow

1. Raw ingestion from Databricks Volumes
2. Bronze layer transformation
3. Silver layer (business logic + joins)
4. Gold layer (aggregations and KPIs)
5. Dashboard visualization

---

## 📈 Key Metrics

- Total Revenue
- Total Orders
- Average Order Value
- Revenue by Category
- Top Customers
- Delivery Performance

---

## 📊 Dashboard

The dashboard provides a business overview of:

- Revenue trends over time
- Product category performance
- Customer behavior
- Order delivery status

(Add screenshot here)

---

## 🔁 Orchestration

The pipeline is automated using Databricks Jobs:

ingestion_raw → bronze_layer → silver_layer → gold_layer

---

## 🚀 How to Run

1. Upload data to Databricks Volume
2. Run notebooks in sequence OR run Databricks Job
3. Access Gold tables
4. Open dashboard

---

## 📌 Project Structure
