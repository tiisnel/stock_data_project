# Stock Market Analysis with Economic Indicators

## Project Overview

This project analyzes the relationship between global economic indicators and stock market performance, focusing on major indices like S&P 500, NASDAQ 100, and Dow Jones Industrial Average. The goal is to provide insights into how economic factors such as GDP growth and inflation impact stock indices.

## Data Sources:

Yahoo Finance: For historical stock data.

World Bank Open Data: For economic indicators like GDP growth and inflation.

## Technical Implementation

Tools and Technologies

Python Libraries:

yfinance for fetching stock data.

wbdata for World Bank data.

duckdb for in-memory database processing.

minio for object storage.

Airflow: To orchestrate the ETL pipeline.

MinIO: For storing raw data as .csv files.

DuckDB: For creating and querying the star schema.

Streamlit: For interactive data visualization.

Docker: To containerize services.

## Project Workflow

### ETL Process:

Fetch Stock Data: Daily updates from Yahoo Finance, storing data as .csv files in MinIO.

Fetch Economic Data: Retrieves GDP growth and inflation data from World Bank API.

### Star Schema Creation:

Dimensions: DimDate, DimStockIndex, DimCountry.

Fact Table: FactMarketEconomicIndicators combines stock and economic data with calculated metrics like daily returns and volatility.

Data stored in MinIO as .parquet files.

### Visualization:

Streamlit provides an interactive dashboard to explore datasets and visualize relationships between stock performance and economic indicators.

## Steps to Set Up and Run

### Environment Setup:

Create a .env file based on the .env.template for storing MinIO credentials.
NB! if on windows, make sure that lines end with LF, not CRLF

### Run ETL Pipeline:

Make composer.sh executable: chmod +x composer.sh.

Start Airflow with ETL tasks: 
./composer.sh init
./composer.sh up.

As airflow runs only once a day, it might be a good idea to trigger first run manually on localhost:8080

### Visualize Data:

Navigate to the Streamlit directory: cd streamlit.

Start the Streamlit app: docker-compose -f docker-compose.streamlit.yml up --build.

Interact with the Dashboard:

Access Streamlit at http://localhost:8501.

## Datasets Page:

Fact table: Combines stock data and economic indicators.

Charts Page:

Dual-axis charts show trends in stock prices and GDP growth.

Filter by stock index and date range


