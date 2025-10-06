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


---

## 🥇 Gold Layer: Star Schema Design

### 🧮 Fact Tables

| Fact Table | Description | Grain |
|-------------|-------------|-------|
| **fact_sales** | Customer sales transactions | 1 row per order |
| **fact_inventory** | Daily inventory snapshots | 1 row per SKU per warehouse per day |
| **fact_returns** | Returned items | 1 row per return |
| **fact_purchases** | Purchase orders to suppliers | 1 row per order |

### 🧩 Dimension Tables

| Dimension | Description |
|------------|-------------|
| **dim_date** | All calendar dates from 2020–2025 |
| **dim_customer** | Customer attributes (industry, location, manager) |
| **dim_product** | Product attributes (category, price, supplier link) |
| **dim_supplier** | Supplier performance data |
| **dim_warehouse** | Warehouse information |

---

## ⚙️ Technologies & Tools

| Category | Tools / Languages |
|-----------|-------------------|
| **Query Language** | SQL |
| **Storage Format** | CSV → Tables (Bronze/Silver/Gold) |
| **Version Control** | GitHub |
| **Future Integrations (Planned)** | Airflow (orchestration), dbt (transformation mgmt), Kafka (streaming ingestion) |

---

## 🧠 Key Features

- ✅ **ETL Pipeline Design (Bronze → Silver → Gold)**
- ✅ **Data Cleansing & Validation**
- ✅ **Schema Enforcement & Type Standardization**
- ✅ **Merge Logic for Incremental Loads**
- ✅ **Dimensional Modeling (Star Schema)**
- ✅ **Analytical Layer for Business Insights**
- ✅ **Scalable Architecture for Future Automation**

---

## 📊 Example Use Cases Enabled

- 🧾 **Sales Performance** → Total revenue by region, product, or time  
- 📦 **Inventory Management** → Detect low-stock and replenishment needs  
- 🔁 **Return Analysis** → Identify high-return products  
- 🚚 **Supplier Evaluation** → Compare suppliers by reliability and lead time  

---

## 📈 Results

After processing, all data is unified in a clean **data warehouse (Gold Layer)** ready for:

- BI dashboards  
- Supply chain KPIs  
- Predictive analytics (future extensions)

**Example KPIs derived:**

- 🧾 Total Sales Revenue  
- 📉 Low Stock Alerts  
- 📈 Supplier Reliability Trends  
- 🔄 Return Rates per Product  

---

## 🔮 Future Enhancements

- 🚀 **Airflow** – Automate ETL pipelines  
- 🧱 **dbt** – Version-controlled SQL transformations  
- 📡 **Kafka** – Real-time streaming data ingestion  
- 📊 **Power BI / Looker** – Visualization layer  

---

## 👨‍💻 Author

**Youssef M. Makram**  
_Data Engineer passionate about building scalable data systems and turning raw data into actionable insights._

📫 [LinkedIn](https://www.linkedin.com/in/youssef-m-makram-m-osman-659a56233/) • [GitHub](https://github.com/YoussefMakram27)

