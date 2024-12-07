from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
import yfinance as yf
import os
from dotenv import load_dotenv
import logging

load_dotenv()

access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")

def get_last_saved_date(bucket_name, prefix="stocks", minio_client=None):
    """
    Get the latest file's date from MinIO bucket based on file naming convention.
    """
    objects = minio_client.list_objects(bucket_name, prefix=prefix)
    latest_date = None
    for obj in objects:
        # filename ormat "stocks:<YYYY-MM-DD>.csv"
        file_date = obj.object_name.split(":")[-1].replace(".csv", "")
        file_date = datetime.strptime(file_date, "%Y-%m-%d").date()
        if not latest_date or file_date > latest_date:
            latest_date = file_date
    return latest_date

def fetch_and_save_data(ds, **kwargs):

    client = Minio(
        "minio:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    bucket_name = "p20"
    prefix = "stocks"
    today = datetime.now().date()

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    last_save =get_last_saved_date(bucket_name, prefix, client)
    if not last_save:
        last_save = datetime(2020, 1, 1).date() # add earlier start date for better analysis
    end_date = today
    if(last_save >=end_date):
        logging.info("up do date")
        return; #up to date
    logging.info(last_save)
    logging.info(end_date)
    tickers = ['^DJI', '^GSPC', 'CME']
    df = yf.download(tickers, start=last_save, end=end_date)
    csv_filename = f"{prefix}:{today}.csv"
    df.to_csv(csv_filename)
    with open(csv_filename, "rb") as file_data:
            client.put_object(
                bucket_name, csv_filename, file_data, os.path.getsize(csv_filename)
            )



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id="fetch_stocks_dag",
    default_args=default_args,
    description="Fetch stock data and save to MinIO",
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 8),
) as dag:
    
    fetch_stocks = PythonOperator(
        task_id="fetch_stocks",
        python_callable=fetch_and_save_data,
        provide_context=True,
    )

fetch_stocks

