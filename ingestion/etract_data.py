import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

engine = create_engine(
    "postgresql://postgres:Waleed21@host.docker.internal:5432/shopflow"
)
# CSV files to load
datasets = {
    "raw_orders": "/opt/airflow/ingestion/raw_data/olist_orders_dataset.csv",
    "raw_customers": "/opt/airflow/ingestion/raw_data/olist_customers_dataset.csv",
    "raw_products": "/opt/airflow/ingestion/raw_data/olist_products_dataset.csv",
    "raw_order_items": "/opt/airflow/ingestion/raw_data/olist_order_items_dataset.csv",
    "raw_order_payments": "/opt/airflow/ingestion/raw_data/olist_order_payments_dataset.csv",
    "raw_order_reviews": "/opt/airflow/ingestion/raw_data/olist_order_reviews_dataset.csv",
    "raw_sellers": "/opt/airflow/ingestion/raw_data/olist_sellers_dataset.csv",
}


def extract_and_load():
    for table_name, file_path in datasets.items():
        print(f"Loading {file_path} into {table_name}...")
        df = pd.read_csv(file_path)
        with engine.begin() as conn:
          df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"✅ {table_name} loaded — {len(df)} rows")

if __name__ == "__main__":
    extract_and_load()