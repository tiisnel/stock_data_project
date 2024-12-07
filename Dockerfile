# Base Airflow image
FROM apache/airflow:2.6.3

# Install required Python packages
RUN pip install --no-cache-dir yfinance pandas minio python-dotenv pymysql
