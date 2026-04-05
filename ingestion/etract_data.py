import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# CSV files to load
datasets = {
    "raw_orders": "/home/waleed/my-etl-pipeline/shopflow/ingestion/raw_data/olist_orders_dataset.csv",
    "raw_customers": "/home/waleed/my-etl-pipeline/shopflow/ingestion/raw_data/olist_customers_dataset.csv",
    "raw_products": "/home/waleed/my-etl-pipeline/shopflow/ingestion/raw_data/olist_products_dataset.csv",
    "raw_order_items": "/home/waleed/my-etl-pipeline/shopflow/ingestion/raw_data/olist_order_items_dataset.csv",
    "raw_order_payments": "/home/waleed/my-etl-pipeline/shopflow/ingestion/raw_data/olist_order_payments_dataset.csv",
    "raw_order_reviews": "/home/waleed/my-etl-pipeline/shopflow/ingestion/raw_data/olist_order_reviews_dataset.csv",
    "raw_sellers": "/home/waleed/my-etl-pipeline/shopflow/ingestion/raw_data/olist_sellers_dataset.csv",
}

def extract_and_load():
    for table_name, file_path in datasets.items():
        print(f"Loading {file_path} into {table_name}...")
        df = pd.read_csv(file_path)
        with engine.connect() as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.commit()
        print(f"✅ {table_name} loaded — {len(df)} rows")

if __name__ == "__main__":
    extract_and_load()