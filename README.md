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

<img width="1761" height="1105" alt="dashboard" src="https://github.com/user-attachments/assets/3c110106-c179-493b-b00f-6d7ca3691a50" />

---

## 🔁 Orchestration

The pipeline is automated using Databricks Jobs:

ingestion_raw → bronze_layer → silver_layer → gold_layer
<img width="1578" height="396" alt="pipe run" src="https://github.com/user-attachments/assets/a0b7c64a-adda-4244-9ac2-a89ffce68668" />

---

## 🚀 How to Run

1. Upload data to Databricks Volume
2. Run notebooks in sequence OR run Databricks Job
3. Access Gold tables
4. Open dashboard

---

## 📁 Project Structure

```text
.
├── images/
│   ├── dashboard.png
│   └── pipeline.png
├── notebooks/
│   ├── 01_ingestion_raw.py
│   ├── 02_bronze_layer.py
│   ├── 03_silver_layer.py
│   └── 04_gold_layer.py
└── README.md
