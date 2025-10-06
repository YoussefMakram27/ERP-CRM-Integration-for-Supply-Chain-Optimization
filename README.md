# 🚀 ERP–CRM Integration for Supply Chain Optimization

### 🧠 End-to-End Data Engineering Project | SQL | Data Warehouse | ETL

![Tech](https://img.shields.io/badge/Tech-Data%20Engineering-blue?style=flat-square)
![SQL](https://img.shields.io/badge/Language-SQL-yellow?style=flat-square)
![ETL](https://img.shields.io/badge/Process-ETL-green?style=flat-square)
![Status](https://img.shields.io/badge/Status-Completed-success?style=flat-square)

---

## 🏭 Problem Statement

Industrial companies often manage their operations through two separate systems:

- **ERP systems** → handle procurement, suppliers, and inventory.  
- **CRM systems** → handle sales, customers, and returns.  

However, these systems operate in **silos**, making it difficult for supply chain managers to:

- Detect **low-stock risks**  
- Forecast **demand trends**  
- Evaluate **supplier performance**  
- Gain a **unified view** of business performance  

---

## 🎯 Project Goal

Design and implement an **end-to-end data pipeline** that integrates ERP and CRM data into a **centralized data warehouse**, enabling:

✅ Historical supply chain analytics  
✅ Data-driven decision-making  
✅ Scalable architecture for future tools (Airflow, dbt, Kafka)

---

## 🧩 Data Sources

All source data were provided as **CSV files** (simulating ERP & CRM exports).

| System | Table | Description |
|---------|--------|-------------|
| **CRM** | `crm_customers` | Customer master data |
| | `crm_sales_orders` | Sales orders by customers |
| | `crm_returns` | Returned items & reasons |
| | `crm_sales_leads` | Lead pipeline information |
| **ERP** | `erp_products` | Product catalog with pricing & supplier info |
| | `erp_suppliers` | Supplier details and reliability |
| | `erp_inventory` | Stock levels per warehouse |
| | `erp_inventory_transactions` | Inventory movements (in/out) |
| | `erp_purchase_orders` | Purchase orders placed with suppliers |

---

## 🧱 Data Architecture

The pipeline follows a **Medallion Architecture** (Bronze → Silver → Gold):

    +------------------------+
    |     CRM / ERP CSVs     |
    +------------------------+
               ↓
      [ BRONZE LAYER ]
      Raw ingestion zone
      - Truncate & Insert from CSVs
      - Minimal transformations
               ↓
      [ SILVER LAYER ]
      Cleansed & standardized data
      - Type casting, deduplication
      - Standard date formats
      - Trim spaces, remove invalid values
      - Added metadata: source_file, load_datetime
      - Merge (UPSERT) operations
               ↓
      [ GOLD LAYER ]
      Business-ready warehouse (Star Schema)
      - 5 Dimensions
      - 4 Fact Tables
               ↓
       Analytics & Insights
