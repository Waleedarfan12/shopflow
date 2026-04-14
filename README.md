ShopFlow Analytics Platform

An end-to-end production-ready e-commerce data pipeline built with modern data engineering tools.

📌 Project Overview
ShopFlow Analytics Platform is a production-ready data engineering project that ingests, transforms, and visualizes Brazilian e-commerce data from Olist. The platform processes over 100,000 orders across 7 datasets and delivers business KPIs through an interactive Power BI dashboard.
The pipeline runs daily on Apache Airflow and follows the Medallion Architecture (Bronze → Silver → Gold) using dbt for data transformation.

🏗️ Architecture
Kaggle Dataset (Brazilian E-commerce)
            ↓
    Python Ingestion Script
            ↓
    PostgreSQL (Bronze Layer)
    Raw tables — 7 datasets
            ↓
    dbt Transformation
    Silver Layer — Cleaned & Joined
            ↓
    dbt Transformation
    Gold Layer — Business KPIs
            ↓
    Apache Airflow (Daily Orchestration)
            ↓
    Power BI Dashboard

🛠️ Tech Stack
LayerToolData SourceKaggle — Brazilian E-commerce (Olist)IngestionPython, PandasData WarehousePostgreSQLTransformationdbt (dbt-postgres)OrchestrationApache Airflow 2.7.1ContainerizationDocker & Docker ComposeVisualizationPower BI

📂 Project Structure
shopflow/
├── ingestion/
│   ├── raw_data/          # Raw CSV files from Kaggle
│   └── etract_data.py     # Python ingestion script
├── dbt/
│   └── shopflow_dbt/
│       ├── models/
│       │   ├── bronze/    # Raw layer — 7 models
│       │   ├── silver/    # Cleaned & joined — 3 models
│       │   └── gold/      # Business KPIs — 4 models
│       ├── macros/        # Custom schema macro
│       └── dbt_project.yml
├── airflow/
│   └── dags/
│       └── shopflow_dags.py   # Main pipeline DAG
├── powerbi/
│   └── shopflow_dashboard.pbix
├── docker-compose.yml
└── README.md

📊 Data Layers
🥉 Bronze Layer — Raw Data
Stores raw data exactly as received from source CSVs with basic type casting.
ModelSourcebronze_ordersolist_orders_dataset.csvbronze_customersolist_customers_dataset.csvbronze_productsolist_products_dataset.csvbronze_order_itemsolist_order_items_dataset.csvbronze_order_paymentsolist_order_payments_dataset.csvbronze_order_reviewsolist_order_reviews_dataset.csvbronze_sellersolist_sellers_dataset.csv
🥈 Silver Layer — Cleaned & Joined
Removes nulls, fixes data types, and joins related tables into meaningful datasets.
ModelDescriptionsilver_ordersOrders joined with customers + delivery statussilver_order_itemsOrder items joined with products and sellerssilver_paymentsPayments with payment category classification
🥇 Gold Layer — Business KPIs
Aggregated business metrics ready for Power BI consumption.
ModelBusiness Valuegold_monthly_revenueMonthly revenue trends and order volumesgold_customer_lifetime_valueCustomer spend, frequency and segmentationgold_top_selling_categoriesBest performing product categories by revenuegold_order_fulfillmentDelivery performance and on-time rate

📈 Power BI Dashboard
The dashboard includes 4 KPI cards and 4 visuals:
KPI Cards:

Total Revenue
Total Orders
Average Order Value
On Time Delivery Rate

Visuals:

Monthly Revenue Trend (Line Chart)
Order Fulfillment Rate (Donut Chart)
Top Selling Categories (Bar Chart)
Customer Lifetime Value (Table)


⚙️ Pipeline Orchestration
The pipeline is orchestrated using Apache Airflow and runs daily in this order:
extract_and_load → bronze_models → silver_models → gold_models
Each task depends on the previous one completing successfully. If any task fails, Airflow retries it once after 5 minutes.

🚀 How to Run
Prerequisites:

Docker & Docker Compose
Python 3.8+
PostgreSQL
Power BI Desktop (Windows)

Steps:
1. Clone the repository:
bashgit clone https://github.com/waleedarfan12/shopflow.git
cd shopflow
2. Download the dataset:
bashpip install kaggle
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d ingestion/raw_data
3. Set up PostgreSQL:
bashsudo -u postgres psql -c "CREATE DATABASE shopflow;"
4. Run ingestion script:
bashpip install pandas sqlalchemy==1.4.46 psycopg2-binary python-dotenv
python ingestion/etract_data.py
5. Run dbt models:
bashcd dbt/shopflow_dbt
dbt run
6. Start Airflow:
bashdocker-compose up -d
Open http://localhost:8080 and trigger the shopflow_pipeline DAG.
7. Connect Power BI:

Open powerbi/shopflow_dashboard.pbix
Connect to your PostgreSQL instance
Refresh the data


📌 Key Design Decisions
Why Medallion Architecture?
Separating data into Bronze, Silver and Gold layers ensures data quality at each stage. Raw data is always preserved in Bronze, making debugging and reprocessing easy.
Why dbt?
dbt brings software engineering best practices to data transformation — version control, testing, and documentation. It is the industry standard tool used by modern data teams.
Why Airflow?
Airflow provides reliable scheduling, dependency management, and monitoring for the pipeline. Running it in Docker ensures reproducibility across environments.
Production considerations:
In a production setup this pipeline would be hosted on cloud infrastructure — PostgreSQL on AWS RDS or GCP Cloud SQL, Airflow on AWS MWAA or GCP Composer, and the gold layer exposed via a data API or BI tool with row-level security.

👨‍💻 Author
Waleed
Data Engineer  Pakistan

GitHub: Waleedarfan12
LinkedIn: https://www.linkedin.com/in/waleed-arfan-b61938316

📄 Dataset
This project uses the Brazilian E-Commerce Public Dataset by Olist available on Kaggle under the CC BY-NC-SA 4.0 license.