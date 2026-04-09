# Google Ads Data Engineering Pipeline 🚀

## 📌 Overview

This project demonstrates an end-to-end data engineering pipeline built on Google Ads data using a Medallion Architecture (Bronze, Silver, Gold).

The pipeline processes raw advertising data, transforms it, and generates business insights along with automated reporting.

---

## 🏗️ Architecture

Google Ads Data → Bronze → Silver → Gold → Analysis → LLM Insights

* **Bronze Layer**: Raw data ingestion from Google Ads stored in data lake format
* **Silver Layer**: Data cleaning, null handling, and currency transformation
* **Gold Layer**: Business-ready data using fact and dimension tables
* **Analysis Layer**: KPI calculations and reporting
* **LLM Layer**: AI-generated insights

---

## 📁 Project Structure

```
google-ads-data-engineering-pipeline/
│
├── data/
├── notebooks/
├── config/
├── docs/
├── requirements.txt
└── README.md
```

---

## ⚙️ Technologies Used

* Python
* Pandas
* SQL
* Data Lake (Parquet/CSV)
* Google Ads Data
* LLM (for reporting)

---

## 🚀 Pipeline Workflow

1. Ingest raw Google Ads data into Bronze layer
2. Clean and transform data in Silver layer
3. Convert currency from micros to rupees
4. Build fact and dimension tables in Gold layer
5. Perform analysis and calculate KPIs
6. Generate automated insights using LLM

---

## 📊 Key Features

* Medallion Architecture (Bronze, Silver, Gold)
* Currency conversion (Micros → Rupees)
* Data cleaning and validation
* Star schema modeling
* KPI metrics (CTR, CPC, Conversion Rate)
* AI-based reporting

---

## 📐 Data Modeling

* **Fact Table**: Clicks, Impressions, Cost, Conversions
* **Dimension Tables**: Campaign, Ad Group, Date
* **Schema**: Star Schema

---

## 🧠 Use Case

This project simulates a real-world marketing analytics pipeline where Google Ads data is processed to generate insights for business decision-making.

---

## ▶️ How to Run

1. Clone the repository
2. Install dependencies
3. Run notebooks in order:

   * 01_bronze
   * 02_silver
   * 03_gold
   * 04_analysis
   * 05_llm_report

---

## 📌 Future Improvements

* Automate using Airflow / scheduling tools
* Integrate real-time streaming
* Build dashboard (Power BI / Tableau)
