import os
import logging
from datetime import datetime
import duckdb
import yfinance as yf
import pandas as pd
from minio import Minio
import wbdata
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# MinIO connection details (replace with your actual credentials)
access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")
minio_endpoint = "minio:9000"

# Function to get the last saved date from MinIO (modified to handle potential issues)
def get_last_saved_date(bucket_name, prefix, minio_client=None):
    """
    Get the latest file's date from MinIO bucket based on file naming convention.
    """
    try:
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        latest_date = None
        for obj in objects:
            file_date_str = obj.object_name.split(":")[-1].replace(".csv", "")
            try:
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                if not latest_date or file_date > latest_date:
                    latest_date = file_date
            except ValueError:
                logging.warning(f"Skipping file with invalid date format: {obj.object_name}")
        return latest_date
    except Exception as e:
        logging.error(f"Error listing objects in bucket {bucket_name}: {e}")
        return None

# Airflow Task: Fetch and save stock data to MinIO
def fetch_and_save_stocks(ds, **kwargs):
    client = Minio(
        minio_endpoint,
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
        last_save = datetime(1990, 1, 1).date()  # Start date for stock analysis
    end_date = today

    if last_save >= end_date:
        logging.info("Stock data up to date")
        return

    logging.info(f"Fetching stock data from {last_save} to {end_date}")
    tickers = ['^DJI', '^GSPC', '^NDX']  # Corrected ticker for NASDAQ 100
    df = yf.download(tickers, start=last_save, end=end_date, group_by="ticker")

    # Correctly reshape the DataFrame
    df = df.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index()
    df.columns = ['Date', 'Ticker', 'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
    df = df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]

    csv_filename = f"{prefix}:{today}.csv"
    df.to_csv(csv_filename, index=False)  # Added index=False
    with open(csv_filename, "rb") as file_data:
        client.put_object(
            bucket_name, csv_filename, file_data, os.path.getsize(csv_filename)
        )
    os.remove(csv_filename)  # Clean up the temporary CSV file


# Airflow Task: Fetch and save World Bank data to MinIO
def fetch_and_save_world_bank_data(ds, **kwargs):
    client = Minio(
        minio_endpoint,
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

    # Use wbdata.get_dataframe to fetch data
    df = wbdata.get_dataframe(indicators, country='USA', convert_date=True, freq='M')


    # Reset the index to make 'Date' a column
    df = df.reset_index()

    # Rename columns for clarity
    df.rename(columns={
        "GDP Growth": "GDPGrowthRate",
        "Inflation, Consumer Prices": "InflationRate"
    }, inplace=True)

    csv_filename = f"world_bank:{today}.csv"
    df.to_csv(csv_filename, index=False)
    with open(csv_filename, "rb") as file_data:
        client.put_object(
            bucket_name, csv_filename, file_data, os.path.getsize(csv_filename)
        )
    os.remove(csv_filename)

# Function to create the star schema in DuckDB
def create_star_schema(ds, **kwargs):
    # Connect to DuckDB (in-memory database)
    client = Minio(
    minio_endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
    )
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    # Install and load the aws extension
    con.execute("INSTALL aws")
    con.execute("LOAD aws")
    con.sql(f"SET s3_url_style=path")
    con.sql(f"SET s3_endpoint='{minio_endpoint}'")
    con.sql(f"SET s3_access_key_id='{access_key}'")
    con.sql(f"SET s3_secret_access_key='{secret_key}'")
    con.sql("SET s3_use_ssl=false")

    # --- 1. Read data from MinIO into DuckDB ---
    source_bucket_name = "p20"  # Source bucket for CSV files
    parquet_bucket_name = "star"  # Destination bucket for Parquet files

    # Make sure the "star" bucket exists
    if not client.bucket_exists(parquet_bucket_name):
        client.make_bucket(parquet_bucket_name)

    # Read the latest stocks CSV
    stocks_df = con.sql(f"""
        SELECT * FROM read_csv_auto('s3://{source_bucket_name}/stocks:*.csv',
        filename=true, header=true, dateformat='%Y-%m-%d')
    """).df()


    # Read the latest world_bank CSV
    world_bank_df = con.sql(f"""
        SELECT * FROM read_csv_auto('s3://{source_bucket_name}/world_bank:*.csv',
        filename=true, header=true, dateformat='%Y-%m-%d')
    """).df()
    # --- 2. Create Dimension Tables ---

    # Date Dimension
    con.sql("""
        CREATE OR REPLACE TABLE DimDate AS
        SELECT DISTINCT
            CAST(Date AS DATE) as DateKey,
            Date as Date,
            YEAR(Date) as Year,
            CAST(QUARTER(Date) AS VARCHAR) as Quarter, -- Added Quarter calculation
            MONTH(Date) as Month,
            CAST(Date AS VARCHAR) as DayOfWeek,
            CASE WHEN strftime(Date, '%w') IN ('0', '6') THEN TRUE ELSE FALSE END as IsWeekend
        FROM stocks_df
        UNION
        SELECT DISTINCT
            CAST(Date AS DATE) as DateKey,
            Date as Date,
            YEAR(Date) as Year,
            CAST(QUARTER(Date) AS VARCHAR) as Quarter, -- Added Quarter calculation
            MONTH(Date) as Month,
            CAST(Date AS VARCHAR) as DayOfWeek,
            CASE WHEN strftime(Date, '%w') IN ('0', '6') THEN TRUE ELSE FALSE END as IsWeekend
        FROM world_bank_df
    """)

    # Stock Index Dimension
    con.sql("""
        CREATE OR REPLACE TABLE DimStockIndex AS
        SELECT DISTINCT
            MD5(Ticker) AS IndexKey, -- Using a hash as a unique key
            Ticker as IndexName,
            CASE
                WHEN Ticker = '^GSPC' THEN 'S&P 500'
                WHEN Ticker = '^NDX' THEN 'NASDAQ 100'
                WHEN Ticker = '^DJI' THEN 'Dow Jones'
                ELSE 'Other'
            END AS IndexCode
        FROM stocks_df
    """)

    # Country Dimension
    con.sql("""
        CREATE OR REPLACE TABLE DimCountry AS
        SELECT 
            'USA' AS CountryKey,
            'United States' AS CountryName,
            'USA' AS CountryCode
    """)

    # --- 3. Create Fact Table ---

    # Calculate Daily Return
    stocks_df['DailyReturn'] = stocks_df.groupby('Ticker')['Close'].pct_change()

    # Calculate Volatility (smaller window, min_periods)
    stocks_df['Volatility'] = stocks_df.groupby('Ticker')['DailyReturn'].transform(
        lambda x: x.rolling(window=5, min_periods=3).std()
    )

    stocks_df.dropna(subset=['DailyReturn'], inplace=True)
    world_bank_df.rename(columns={
        "GDP Growth": "GDPGrowthRate",
        "Inflation, Consumer Prices": "InflationRate"
    }, inplace=True)
    con.sql("""
        CREATE OR REPLACE TABLE AnnualStockData AS
        SELECT
            dsi.IndexKey,
            dc.CountryKey,
            STRFTIME(CAST(s.Date as DATE), '%Y') AS Year,  -- Extract year for aggregation
            AVG(s.Open) AS AvgOpen,
            AVG(s.High) AS AvgHigh,
            AVG(s.Low) AS AvgLow,
            AVG(s.Close) AS AvgClose,
            AVG(s.Volume) AS AvgVolume,
            AVG(s.Volatility) AS AvgVolatility  -- Calculate average volatility for the year
        FROM stocks_df s
        JOIN DimStockIndex dsi ON MD5(s.Ticker) = dsi.IndexKey
        JOIN DimCountry dc ON dc.CountryCode = 'USA'
        GROUP BY 1, 2, 3  -- Group by IndexKey, CountryKey, Year
    """)

    # --- Join with World Bank Data ---
    con.sql("""
        CREATE OR REPLACE TABLE FactMarketEconomicIndicators AS
        SELECT
            asd.Year,
            asd.IndexKey,
            asd.CountryKey,
            asd.AvgOpen,
            asd.AvgHigh,
            asd.AvgLow,
            asd.AvgClose,
            asd.AvgVolume,
            asd.AvgVolatility,
            wb.GDPGrowthRate,
            wb.InflationRate
        FROM AnnualStockData asd
        LEFT JOIN world_bank_df wb ON asd.Year = STRFTIME(CAST(wb.Date as DATE), '%Y')
    """)
    # --- 4. Export Fact Table to Parquet (to MinIO) ---
    con.sql(f"""
        COPY FactMarketEconomicIndicators 
        TO 's3://{parquet_bucket_name}/fact_table.parquet' 
        (FORMAT PARQUET);
    """)

    # --- (Optional) Export dimension tables to MinIO as well ---
    con.sql(f"""
        COPY DimDate 
        TO 's3://{parquet_bucket_name}/dim_date.parquet' 
        (FORMAT PARQUET);
    """)

    con.sql(f"""
        COPY DimStockIndex 
        TO 's3://{parquet_bucket_name}/dim_stock_index.parquet' 
        (FORMAT PARQUET);
    """)

    con.sql(f"""
        COPY DimCountry
        TO 's3://{parquet_bucket_name}/dim_country.parquet' 
        (FORMAT PARQUET);
    """)

    
    # --- 5. Verify the Schema ---
    print("DimDate Table:")
    print(con.sql("SELECT * FROM DimDate LIMIT 5").df())

    print("DimStockIndex Table:")
    print(con.sql("SELECT * FROM DimStockIndex").df())

    print("DimCountry Table:")
    print(con.sql("SELECT * FROM DimCountry").df())

    print("FactMarketEconomicIndicators Table:")
    print(con.sql("SELECT * FROM FactMarketEconomicIndicators LIMIT 5").df())

    con.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id="stock_market_star_schema",
    schedule_interval="0 0 * * *",  # Run daily at midnight, adjust as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["stock_market", "star_schema", "duckdb"],
) as dag:
    fetch_stocks_task = PythonOperator(
        task_id="fetch_and_save_stocks",
        python_callable=fetch_and_save_stocks,
        provide_context=True,
    )

    fetch_world_bank_task = PythonOperator(
        task_id="fetch_and_save_world_bank_data",
        python_callable=fetch_and_save_world_bank_data,
        provide_context=True,
    )

    create_schema_task = PythonOperator(
        task_id="create_star_schema",
        python_callable=create_star_schema,
        provide_context=True,
    )

    fetch_stocks_task >> fetch_world_bank_task >> create_schema_task
