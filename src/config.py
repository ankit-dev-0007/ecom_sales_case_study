"""Configuration settings for the e-commerce data processing pipeline."""

# Catalog and Schema Configuration
CATALOG_NAME = "ecommerce_sales"

# Schema Names
SCHEMA_SOURCE = "source_data_layer"
SCHEMA_BRONZE = "bronze_layer"
SCHEMA_SILVER = "processed_layer"
SCHEMA_GOLD = "gold_layer"

# Volume Names
VOLUME_BRONZE = "ecom_volume_raw"
VOLUME_SILVER = "ecom_volume_processed"
VOLUME_GOLD = "ecom_volume_gold"

# File Paths
RAW_BASE_PATH = "/Volumes/ecommerce_sales/bronze_layer/ecom_volume_raw/source_files/"
BRONZE_DELTA_PATH = "/Volumes/ecommerce_sales/bronze_layer/ecom_volume_raw"
SILVER_DELTA_PATH = "/Volumes/ecommerce_sales/processed_layer/ecom_volume_processed"
GOLD_DELTA_PATH = "/Volumes/ecommerce_sales/gold_layer/ecom_volume_gold"

# Source File Names
PRODUCTS_FILE = "Products.csv"
CUSTOMERS_FILE = "Customer.xlsx"
ORDERS_FILE = "Orders.json"
