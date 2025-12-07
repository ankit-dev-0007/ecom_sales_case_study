"""Data ingestion module for loading raw data from various sources."""

import logging
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from src.utils import sanitize_column, save_as_delta_table
from src.config import (
    RAW_BASE_PATH, BRONZE_DELTA_PATH, CATALOG_NAME,
    PRODUCTS_FILE, CUSTOMERS_FILE, ORDERS_FILE
)

logger = logging.getLogger(__name__)


def load_products_data(spark: SparkSession) -> DataFrame:
    """
    Load products data from CSV file.
    
    Args:
        spark: Active Spark session
        
    Returns:
        DataFrame containing raw products data
    """
    products_path = f"{RAW_BASE_PATH}/{PRODUCTS_FILE}"
    
    raw_products_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("quote", "\"")
        .option("escape", "\"")
        .csv(products_path)
    )
    
    raw_products_df = sanitize_column(raw_products_df)
    logger.info(f"Loaded products data with columns: {raw_products_df.columns}")
    
    # Save to bronze layer
    save_as_delta_table(
        raw_products_df,
        "products_raw",
        BRONZE_DELTA_PATH,
        "bronze_layer",
        CATALOG_NAME
    )
    
    return raw_products_df


def load_customers_data(spark: SparkSession) -> DataFrame:
    """
    Load customers data from Excel file.
    
    Note: Uses pandas and openpyxl for Excel processing as spark-excel
    is not supported in serverless environments.
    
    Args:
        spark: Active Spark session
        
    Returns:
        DataFrame containing raw customers data
    """
    customers_path = f"{RAW_BASE_PATH}/{CUSTOMERS_FILE}"
    
    # Read Excel file using pandas
    raw_customers_pd = pd.read_excel(customers_path, engine="openpyxl")
    
    # Ensure 'phone' column is string type
    raw_customers_pd["phone"] = raw_customers_pd["phone"].astype(str)
    
    # Convert to Spark DataFrame
    raw_customers_df = spark.createDataFrame(raw_customers_pd)
    
    # Standardize column names
    raw_customers_df = sanitize_column(raw_customers_df)
    
    # Save to bronze layer
    save_as_delta_table(
        raw_customers_df,
        "customers_raw",
        BRONZE_DELTA_PATH,
        "bronze_layer",
        CATALOG_NAME
    )
    
    return raw_customers_df


def load_orders_data(spark: SparkSession) -> DataFrame:
    """
    Load orders data from JSON file.
    
    Args:
        spark: Active Spark session
        
    Returns:
        DataFrame containing raw orders data
    """
    orders_path = f"{RAW_BASE_PATH}/{ORDERS_FILE}"
    
    try:
        raw_orders_df = spark.read \
            .format("json") \
            .option("multiline", "true") \
            .load(orders_path)
    except Exception as e:
        logger.error(f"Error loading orders data: {str(e)}")
        raise
    
    # Standardize column names
    raw_orders_df = sanitize_column(raw_orders_df)
    
    # Save to bronze layer
    save_as_delta_table(
        raw_orders_df,
        "orders_raw",
        BRONZE_DELTA_PATH,
        "bronze_layer",
        CATALOG_NAME
    )
    
    return raw_orders_df
