from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import duckdb
impart pandas as pd
import os
from dotenv import load_dotenv
import logging
from wbdata import get_dataframe
import yfinance as yf

load_dotenv()

access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")

def get_last_saved_date(bucket_name, prefix, minio_client=None):
    """
    Get the latest file's date from MinIO bucket based on file naming convention.
    """
    objects = minio_client.list_objects(bucket_name, prefix=prefix)
    latest_date = None
    for obj in objects:
        # filename format "<prefix>:<YYYY-MM-DD>.csv"
        file_date = obj.object_name.split(":")[-1].replace(".csv", "")
        file_date = datetime.strptime(file_date, "%Y-%m-%d").date()
        if not latest_date or file_date > latest_date:
            latest_date = file_date
    return latest_date

def fetch_and_save_stocks(ds, **kwargs):
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
    last_save = get_last_saved_date(bucket_name, prefix, client)
    if not last_save:
        last_save = datetime(2020, 1, 1).date()  # Start date for stock analysis
    end_date = today
    if last_save >= end_date:
        logging.info("Stock data up to date")
        return
    logging.info(f"Fetching stock data from {last_save} to {end_date}")
    tickers = ['^DJI', '^GSPC', 'CME']
    df = yf.download(tickers, start=last_save, end=end_date)
    csv_filename = f"{prefix}:{today}.csv"
    df.to_csv(csv_filename)
    with open(csv_filename, "rb") as file_data:
        client.put_object(
            bucket_name, csv_filename, file_data, os.path.getsize(csv_filename)
        )

def fetch_and_save_world_bank_data(ds, **kwargs):
    client = Minio(
        "minio:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    bucket_name = "p20"
    today = datetime.now().date()

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    indicators = {
        "NY.GDP.MKTP.KD.ZG": "GDP Growth",
        "FP.CPI.TOTL": "Inflation, Consumer Prices"
    }

    logging.info("Fetching World Bank data")
    df = get_dataframe(indicators, convert_date=True)
    csv_filename = f"world_bank:{today}.csv"
    df.to_csv(csv_filename)
    with open(csv_filename, "rb") as file_data:
        client.put_object(
            bucket_name, csv_filename, file_data, os.path.getsize(csv_filename)
        )

def create_duckdb_tables(ds, **kwargs):
    client = Minio(
        "minio:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    bucket_name = "p20"
    duckdb_bucket = "duckdb"
    today = datetime.now().date()

    if not client.bucket_exists(duckdb_bucket):
        client.make_bucket(duckdb_bucket)

    # Connect DuckDB to MinIO S3 bucket
    con = duckdb.connect(database=":memory:")
    s3_config = {
        "s3_url_style": "path",
        "s3_use_ssl": "false",
        "s3_access_key_id": access_key,
        "s3_secret_access_key": secret_key,
        "s3_endpoint": "minio:9000"
    }
    for key, value in s3_config.items():
        con.execute(f"SET {key}='{value}'")

    # Load CSV files directly from S3 into DuckDB
    stocks_table = f"stocks_{today}"
    world_bank_table = f"world_bank_{today}"

    con.execute(f"CREATE TABLE stocks AS SELECT * FROM read_csv_auto('s3://{bucket_name}/stocks:{today}.csv')")
    con.execute(f"CREATE TABLE world_bank AS SELECT * FROM read_csv_auto('s3://{bucket_name}/world_bank:{today}.csv')")

    # Export tables to Parquet format
    con.execute(f"COPY stocks TO 's3://{duckdb_bucket}/stocks_{today}.parquet' (FORMAT 'parquet')")
    con.execute(f"COPY world_bank TO 's3://{duckdb_bucket}/world_bank_{today}.parquet' (FORMAT 'parquet')")

def load_data_from_bucket(ds, **kwargs):
    client = Minio(
        "minio:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    bucket_name = "duckdb"

    # List all objects in the bucket
    objects = client.list_objects(bucket_name)
    for obj in objects:
        logging.info(f"Found file: {obj.object_name}")

    # Example: Download and load Parquet files
    stocks_file = f"stocks_{datetime.now().date()}.parquet"
    world_bank_file = f"world_bank_{datetime.now().date()}.parquet"

    client.fget_object(bucket_name, stocks_file, stocks_file)
    client.fget_object(bucket_name, world_bank_file, world_bank_file)

    # Load into pandas DataFrame
    stocks_df = pd.read_parquet(stocks_file)
    world_bank_df = pd.read_parquet(world_bank_file)

    logging.info(f"Stocks DataFrame: {stocks_df.head()}")
    logging.info(f"World Bank DataFrame: {world_bank_df.head()}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id="fetch_data_dag",
    default_args=default_args,
    description="Fetch stock and World Bank data and save to MinIO",
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 8),
) as dag:

    fetch_stocks = PythonOperator(
        task_id="fetch_stocks",
        python_callable=fetch_and_save_stocks,
        provide_context=True,
    )

    fetch_world_bank_data = PythonOperator(
        task_id="fetch_world_bank_data",
        python_callable=fetch_and_save_world_bank_data,
        provide_context=True,
    )

    create_duckdb = PythonOperator(
        task_id="create_duckdb",
        python_callable=create_duckdb_tables,
        provide_context=True,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_from_bucket,
        provide_context=True,
    )

fetch_stocks >> fetch_world_bank_data >> create_duckdb >> load_data

