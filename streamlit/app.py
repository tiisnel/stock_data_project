import duckdb
import streamlit as st
from minio import Minio
import pandas as pd
import matplotlib.pyplot as plt
import os

# MinIO connection details (replace with your actual credentials)
access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")
minio_endpoint = "minio:9000"

# Initialize MinIO client
client = Minio(
    minio_endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

# DuckDB setup
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Configure DuckDB S3 settings
s3_config = {
    's3_url_style': 'path',
    's3_endpoint': minio_endpoint,
    's3_access_key_id': access_key,
    's3_secret_access_key': secret_key,
    's3_use_ssl': 'false',
}

for key, value in s3_config.items():
    con.execute(f"SET {key}='{value}';")

# Sidebar Navigation
st.sidebar.title("Navigation")
if st.sidebar.button("Datasets"):
    st.session_state["page"] = "Datasets"

if st.sidebar.button("Charts"):
    st.session_state["page"] = "Charts"


# Set default page to Datasets
if "page" not in st.session_state:
    st.session_state["page"] = "Datasets"

# Page: Datasets
if st.session_state["page"] == "Datasets":
    st.title("Datasets Page")
    
    st.subheader("Fact table")
    parquet_bucket_name = "star"
    file_name = "fact_table.parquet"
    fact_table_file_path = f"s3://{parquet_bucket_name}/{file_name}"
    fact_table_data = con.sql(f"SELECT * FROM read_parquet('{fact_table_file_path}')").df()
    st.write(fact_table_data)

    st.subheader("Stock dimension table")
    file_name = "dim_stock_index.parquet"
    stock_table_file_path = f"s3://{parquet_bucket_name}/{file_name}"
    stock_table_data = con.sql(f"SELECT * FROM read_parquet('{stock_table_file_path}')").df()
    st.write(stock_table_data)

    st.subheader("Country dimension table")
    file_name = "dim_country.parquet"
    country_table_file_path = f"s3://{parquet_bucket_name}/{file_name}"
    country_table_data = con.sql(f"SELECT * FROM read_parquet('{country_table_file_path}')").df()
    st.write(country_table_data)

    st.subheader("Date dimension table")
    file_name = "dim_date.parquet"
    date_table_file_path = f"s3://{parquet_bucket_name}/{file_name}"
    date_table_data = con.sql(f"SELECT * FROM read_parquet('{date_table_file_path}')").df()
    st.write(date_table_data)


# Page: Charts
elif st.session_state["page"] == "Charts":
    st.title("Charts Page")
    st.write("This page allows you to filter and view Charts.")

    # Load and filter data
    parquet_bucket_name = "star"
    fact_table_file_name = "fact_table.parquet"
    fact_table_file_path = f"s3://{parquet_bucket_name}/{fact_table_file_name}"
    fact_table_data = con.sql(f"SELECT * FROM read_parquet('{fact_table_file_path}') ORDER BY DateKey").df()

    dim_stock_file_name = "dim_stock_index.parquet"
    dim_stock_file_path = f"s3://{parquet_bucket_name}/{dim_stock_file_name}"
    dim_stock = con.sql(f"SELECT * FROM read_parquet('{dim_stock_file_path}')").df()

    # Create filters
    index_mapping = dict(zip(dim_stock['IndexCode'], dim_stock['IndexKey']))
    selected_index_name = st.sidebar.selectbox('Select Index', list(index_mapping.keys()))
    selected_index_id = index_mapping[selected_index_name]

    min_date = fact_table_data['DateKey'].min()
    max_date = fact_table_data['DateKey'].max()
    start_date, end_date = st.sidebar.date_input('Select Date Range', [min_date, max_date])

    # Apply filters
    filtered_data = fact_table_data[
        (fact_table_data['IndexKey'] == selected_index_id) &
        (fact_table_data['DateKey'] >= pd.to_datetime(start_date)) &
        (fact_table_data['DateKey'] <= pd.to_datetime(end_date))
    ]

    # Display filtered data and chart
    if not filtered_data.empty:
        st.subheader(f"Close Price and GDP Growth - {selected_index_name}")
        fig, ax1 = plt.subplots()

        color = 'tab:blue'
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Close Price', color=color)
        ax1.plot(filtered_data['DateKey'], filtered_data['Close'], color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        ax2 = ax1.twinx()
        color = 'tab:red'
        ax2.set_ylabel('GDP Growth Rate (%)', color=color)
        ax2.plot(filtered_data['DateKey'], filtered_data['GDPGrowthRate'], color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout()
        st.pyplot(fig)
    else:
        st.warning(f"No data found for the selected index: {selected_index_name}")

