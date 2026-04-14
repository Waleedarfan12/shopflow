🚀 ShopFlow Analytics Platform

An end-to-end production-style data engineering pipeline that ingests, transforms, and visualizes Brazilian e-commerce data using modern data stack tools.

📌 Problem Statement

E-commerce platforms generate large volumes of fragmented data across orders, payments, customers, and products.
This makes it difficult to track business performance, customer behavior, and operational efficiency.

👉 ShopFlow solves this by building a fully automated data pipeline that transforms raw data into actionable business insights.

🎯 Project Goals
Build a scalable end-to-end data pipeline
Implement Medallion Architecture (Bronze → Silver → Gold)
Automate workflows using Apache Airflow
Transform data using dbt with modular models
Deliver business insights via Power BI dashboard
🏗️ Architecture
Kaggle Dataset (Olist E-commerce)
        ↓
Python Ingestion Layer
        ↓
PostgreSQL (Bronze Layer - Raw Data)
        ↓
dbt Transformations
        ↓
Silver Layer (Cleaned & Joined Data)
        ↓
Gold Layer (Business KPIs & Aggregations)
        ↓
Apache Airflow (Orchestration - Daily Runs)
        ↓
Power BI Dashboard (Analytics & Insights)
🛠️ Tech Stack
Layer	Tools / Technologies
Data Source	Kaggle (Olist Dataset)
Ingestion	Python, Pandas
Data Warehouse	PostgreSQL
Transformation	dbt (dbt-postgres)
Orchestration	Apache Airflow
Containerization	Docker, Docker Compose
Visualization	Power BI


📂 Project Structure
shopflow/
│
├── ingestion/
│   ├── raw_data/                # Kaggle datasets (CSV)
│   └── extract_data.py         # Data ingestion script
│
├── dbt/
│   └── shopflow_dbt/
│       ├── models/
│       │   ├── bronze/        # Raw layer models
│       │   ├── silver/        # Cleaned & joined models
│       │   └── gold/          # Business KPI models
│       ├── macros/
│       └── dbt_project.yml
│
├── airflow/
│   └── dags/
│       └── shopflow_dag.py     # Pipeline orchestration
│
├── powerbi/
│   └── shopflow_dashboard.pbix
│
├── docker-compose.yml
└── README.md
🥉 Bronze Layer (Raw Data)



Stores raw ingested data without transformation.

Orders
Customers
Products
Order Items
Payments
Reviews
Sellers

👉 Purpose: Preserve original data for traceability and reprocessing.

🥈 Silver Layer (Cleaned Data)

Cleaned, standardized, and joined datasets.

Null handling
Data type corrections
Table joins

Examples:

Orders + Customers → enriched order dataset
Order Items + Products → product-level transactions
🥇 Gold Layer (Business KPIs)

Final analytics-ready datasets for BI tools.

Monthly Revenue Trends
Customer Lifetime Value (CLV)
Top Selling Categories
Order Fulfillment Performance
📊 Power BI Dashboard

The dashboard provides business visibility through KPIs and visuals:

KPI Metrics:
Total Revenue
Total Orders
Average Order Value
On-time Delivery Rate
Visuals:
Monthly Revenue Trend (Line Chart)
Order Fulfillment (Donut Chart)
Top Categories (Bar Chart)
Customer Lifetime Value (Table)
⚙️ Orchestration (Apache Airflow)


Pipeline is scheduled and automated using Airflow DAG:

extract_and_load
      ↓
bronze_models
      ↓
silver_models
      ↓
gold_models
Features:
Daily scheduled runs
Task dependencies
Retry mechanism (1 retry after failure)
Monitoring via Airflow UI
🚀 How to Run the Project
1️⃣ Clone Repository
git clone https://github.com/waleedarfan12/shopflow.git
cd shopflow
2️⃣ Install Dataset
pip install kaggle

kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d ingestion/raw_data
3️⃣ Setup PostgreSQL
CREATE DATABASE shopflow;
4️⃣ Run Ingestion
pip install pandas sqlalchemy psycopg2-binary python-dotenv

python ingestion/extract_data.py
5️⃣ Run dbt Models
cd dbt/shopflow_dbt
dbt run
6️⃣ Start Airflow
docker-compose up -d

Open:

http://localhost:8080

Trigger:

shopflow_dag
7️⃣ Power BI Dashboard
Open powerbi/shopflow_dashboard.pbix
Connect to PostgreSQL
Refresh data
💡 Key Engineering Decisions
📌 Why Medallion Architecture?

Ensures:

Data quality at each stage
Easier debugging
Reusable datasets
Scalable design
📌 Why dbt?
Modular transformations
Version-controlled SQL models
Testing & documentation
Industry-standard in analytics engineering
📌 Why Airflow?
Workflow orchestration
Dependency management
Retry & monitoring
Production-grade scheduling
🧠 Production Considerations

In a real-world cloud setup:

PostgreSQL → AWS RDS / GCP Cloud SQL
Airflow → MWAA / Cloud Composer
dbt → CI/CD pipelines
BI Layer → Power BI / Tableau / Looker
Data API layer for external consumption
👨‍💻 Author

Waleed – Data Engineer (Pakistan 🇵🇰)

GitHub: https://github.com/waleedarfan12
LinkedIn: https://www.linkedin.com/in/waleed-arfan-b61938316
📄 Dataset

Brazilian E-commerce Public Dataset (Olist)
Source: Kaggle
License: CC BY-NC-SA 4.0