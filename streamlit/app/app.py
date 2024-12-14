import streamlit as st
import logging
import duckdb
import pandas as pd
from minio import Minio
import os
import plotly.express as px

# --- MinIO Connection Details ---
access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")
minio_endpoint = "minio:9000"

# Configure Streamlit's logger
st_logger = logging.getLogger("streamlit")
st_logger.setLevel(logging.DEBUG)

# Add a Streamlit handler to output logs to the Streamlit app
st_handler = logging.StreamHandler(st.sidebar)
st_handler.setLevel(logging.DEBUG)
st_logger.addHandler(st_handler)

# --- DuckDB Connection and Setup ---
@st.cache_resource
def get_duckdb_conn():
    try:
        con = duckdb.connect()
        con.execute("INSTALL aws")
        con.execute("LOAD aws")
        con.sql(f"SET s3_url_style=path")
        con.sql(f"SET s3_endpoint='{minio_endpoint}'")
        con.sql(f"SET s3_access_key_id='{access_key}'")
        con.sql(f"SET s3_secret_access_key='{secret_key}'")
        con.sql("SET s3_use_ssl=false")
        st_logger.info("Successfully connected to DuckDB and configured AWS extension.")
        return con
    except Exception as e:
        st_logger.error(f"Error setting up DuckDB connection: {e}")
        st.error(f"Failed to connect to DuckDB. Check logs for details.")
        return None

con = get_duckdb_conn()

# --- Load Data from MinIO ---
parquet_bucket_name = "star"

try:
    fact_table = con.sql(f"SELECT * FROM read_parquet('s3://{parquet_bucket_name}/fact_table.parquet')").df()
    dim_stock_index = con.sql(f"SELECT * FROM read_parquet('s3://{parquet_bucket_name}/dim_stock_index.parquet')").df()

    st_logger.debug(f"Successfully loaded data from MinIO. Fact table shape: {fact_table.shape}, Dim stock index shape: {dim_stock_index.shape}")
    # --- Data Preprocessing (Joins) ---
    merged_data = pd.merge(fact_table, dim_stock_index, on="IndexKey")
    st_logger.debug(f"Merged data shape: {merged_data.shape}")

    # Calculate year-over-year percentage change
    merged_data["AvgClose_PctChange"] = merged_data.groupby("IndexName")["AvgClose"].pct_change(fill_method=None) * 100
    merged_data["GDPGrowthRate_PctChange"] = merged_data["GDPGrowthRate"].pct_change(fill_method=None) * 100
    merged_data["InflationRate_PctChange"] = merged_data["InflationRate"].pct_change(fill_method=None) * 100

    # --- Streamlit App ---
    st.title("Stock Market and Economic Indicators Analysis (Annual Data)")

    # Add a slider to select the years to display
    years = sorted(merged_data["Year"].unique())
    start_year, end_year = st.select_slider(
        "Select a range of years:",
        options=years,
        value=(years[0], years[-1])  # Default to all years
    )
    # Filter the data based on the selected years
    filtered_data = merged_data[(merged_data["Year"] >= start_year) & (merged_data["Year"] <= end_year)]

    # --- Question 1: Stock Market Trends and GDP Growth ---
    st.header("1. Can stock market trends predict future GDP growth?")

    st.subheader("Correlation between Stock Index Annual Average Close and GDP Growth")
    correlations_gdp = filtered_data.groupby("IndexName", group_keys=False).apply(lambda x: x["AvgClose"].corr(x["GDPGrowthRate"])).reset_index(name="Correlation with GDP Growth")
    st.table(correlations_gdp)

    # Scatter Plot (GDP Growth vs. Avg Close)
    st.subheader("Scatter Plot: GDP Growth Rate vs. Stock Index Average Close")
    for index_name in filtered_data["IndexName"].unique():
        subset = filtered_data[filtered_data["IndexName"] == index_name]
        st.scatter_chart(subset, x="GDPGrowthRate", y="AvgClose", color="IndexName")

    # --- Question 2: Inflation Rates and Stock Market Volatility ---
    st.header("2. How do inflation rates affect stock market volatility?")

    st.subheader("Correlation between Inflation and Stock Index Volatility")
    correlations_inflation = filtered_data.groupby("IndexName", group_keys=False).apply(lambda x: x["InflationRate"].corr(x["AvgVolatility"])).reset_index(name="Correlation with Volatility")
    st.table(correlations_inflation)

    # Scatter Plot (Inflation Rate vs. Avg Volatility)
    st.subheader("Scatter Plot: Inflation Rate vs. Stock Index Average Volatility")
    for index_name in filtered_data["IndexName"].unique():
        subset = filtered_data[filtered_data["IndexName"] == index_name]
        st.scatter_chart(subset, x="InflationRate", y="AvgVolatility", color="IndexName")

    # --- Question 3: Stock Market Response to GDP Changes ---
    st.header("3. Do the stock indices respond differently to changes in GDP?")

    # Create a faceted chart using Plotly Express
    fig = px.line(filtered_data, x="Year", y="AvgClose", title=f"GDP Growth and Avg Close", facet_col="IndexName", color="IndexName", hover_data={"Year":True,"AvgClose":":.2f","GDPGrowthRate":":.2f","IndexName":True})

    # Add GDP growth rate on a secondary axis for each facet
    for i, index_name in enumerate(filtered_data["IndexName"].unique()):
        fig.add_scatter(x=filtered_data[filtered_data["IndexName"] == index_name]["Year"], y=filtered_data[filtered_data["IndexName"] == index_name]["GDPGrowthRate"], mode="lines", name=f"{index_name} GDP Growth Rate", yaxis="y2", row=1, col=i+1, hovertemplate="Year: %{x}<br>GDP Growth Rate: %{y:.2f}")

    # Customize layout for dual axes
    for i in range(1, len(filtered_data["IndexName"].unique()) + 1):
        fig.update_yaxes(title_text="Avg Close", row=1, col=i)
        fig.update_yaxes(title_text="GDP Growth Rate", secondary_y=True, row=1, col=i)

    st.plotly_chart(fig)

    # Create a faceted chart using Plotly Express
    fig = px.line(filtered_data, x="Year", y="AvgClose_PctChange", title=f"YoY GDP Growth and Avg Close", facet_col="IndexName", color="IndexName", hover_data={"Year":True,"AvgClose_PctChange":":.2f","GDPGrowthRate_PctChange":":.2f","IndexName":True})

    # Add GDP growth rate on a secondary axis for each facet
    for i, index_name in enumerate(filtered_data["IndexName"].unique()):
        fig.add_scatter(x=filtered_data[filtered_data["IndexName"] == index_name]["Year"], y=filtered_data[filtered_data["IndexName"] == index_name]["GDPGrowthRate_PctChange"], mode="lines", name=f"{index_name} YoY GDP Growth Rate", yaxis="y2", row=1, col=i+1, hovertemplate="Year: %{x}<br>GDP Growth Rate YoY Change: %{y:.2f}")

    # Customize layout for dual axes and log scale
    for i in range(1, len(filtered_data["IndexName"].unique()) + 1):
        fig.update_yaxes(title_text="Avg Close YoY Change", type="log", row=1, col=i)
        fig.update_yaxes(title_text="GDP Growth Rate YoY Change", secondary_y=True, type="log", row=1, col=i)

    st.plotly_chart(fig)

    # --- Question 4: Stock Market Response to Inflation Changes ---
    st.header("4. Do the stock indices respond differently to changes in inflation rates?")

    # Create a faceted chart using Plotly Express
    fig = px.line(filtered_data, x="Year", y="AvgClose", title=f"Inflation and Avg Close", facet_col="IndexName", color="IndexName", hover_data={"Year":True,"AvgClose":":.2f","InflationRate":":.2f","IndexName":True})

    # Add inflation rate on a secondary axis for each facet
    for i, index_name in enumerate(filtered_data["IndexName"].unique()):
        fig.add_scatter(x=filtered_data[filtered_data["IndexName"] == index_name]["Year"], y=filtered_data[filtered_data["IndexName"] == index_name]["InflationRate"], mode="lines", name=f"{index_name} Inflation Rate", yaxis="y2", row=1, col=i+1, hovertemplate="Year: %{x}<br>Inflation Rate: %{y:.2f}")

    # Customize layout for dual axes
    for i in range(1, len(filtered_data["IndexName"].unique()) + 1):
        fig.update_yaxes(title_text="Avg Close", row=1, col=i)
        fig.update_yaxes(title_text="Inflation Rate", secondary_y=True, row=1, col=i)

    st.plotly_chart(fig)

    # Create a faceted chart using Plotly Express
    fig = px.line(filtered_data, x="Year", y="AvgClose_PctChange", title=f"YoY Inflation and Avg Close", facet_col="IndexName", color="IndexName", hover_data={"Year":True,"AvgClose_PctChange":":.2f","InflationRate_PctChange":":.2f","IndexName":True})

    # Add inflation rate on a secondary axis for each facet
    for i, index_name in enumerate(filtered_data["IndexName"].unique()):
        fig.add_scatter(x=filtered_data[filtered_data["IndexName"] == index_name]["Year"], y=filtered_data[filtered_data["IndexName"] == index_name]["InflationRate_PctChange"], mode="lines", name=f"{index_name} YoY Inflation Rate", yaxis="y2", row=1, col=i+1, hovertemplate="Year: %{x}<br>Inflation Rate YoY Change: %{y:.2f}")

    # Customize layout for dual axes and log scale
    for i in range(1, len(filtered_data["IndexName"].unique()) + 1):
        fig.update_yaxes(title_text="Avg Close YoY Change", type="log", row=1, col=i)
        fig.update_yaxes(title_text="Inflation Rate YoY Change", secondary_y=True, type="log", row=1, col=i)

    st.plotly_chart(fig)

except Exception as e:
    st_logger.error(f"Error loading or processing data: {e}")
    st.error(f"Error loading or processing data. Check logs for details.")

