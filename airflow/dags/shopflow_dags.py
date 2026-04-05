from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys

default_args = {
    'owner': 'waleed',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_extraction():
    subprocess.run([
        sys.executable,
        '/opt/airflow/ingestion/etract_data.py'
    ], check=True)

with DAG(
    dag_id='shopflow_pipeline',
    default_args=default_args,
    description='ShopFlow E-commerce Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_and_load',
        python_callable=run_extraction,
    )

    bronze_task = BashOperator(
        task_id='run_bronze_models',
        bash_command='/home/airflow/.local/bin/dbt run --models bronze --project-dir /opt/airflow/dbt --profiles-dir /home/airflow/.dbt',
    )

    silver_task = BashOperator(
        task_id='run_silver_models',
        bash_command='/home/airflow/.local/bin/dbt run --models silver --project-dir /opt/airflow/dbt --profiles-dir /home/airflow/.dbt',
    )

    gold_task = BashOperator(
        task_id='run_gold_models',
        bash_command='/home/airflow/.local/bin/dbt run --models gold --project-dir /opt/airflow/dbt --profiles-dir /home/airflow/.dbt',
    )

    # Pipeline order: extract → bronze → silver → gold
    extract_task >> bronze_task >> silver_task >> gold_task