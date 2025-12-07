%md
# E-commerce Data Processing Case Study

This repository contains a Databricks notebook for processing e-commerce sales data using PySpark and Delta Lake. The project demonstrates a data lakehouse architecture with bronze, silver, and gold layers for data ingestion, processing, and aggregation.

## Overview

The notebook implements an end-to-end data pipeline for e-commerce data, including:

- **Data Ingestion**: Reading raw data from CSV, Excel, and JSON files
- **Data Quality**: Cleaning, standardizing, and enriching customer, product, and order data
- **Data Transformation**: Joining datasets and creating derived metrics
- **Data Aggregation**: Generating business insights and profit calculations
- **Data Storage**: Saving processed data as Delta tables in a multi-layer architecture

## Architecture

The project follows a medallion architecture:

- **Bronze Layer**: Raw data ingestion with minimal transformations
- **Silver Layer**: Cleaned and enriched data
- **Gold Layer**: Aggregated business metrics

## Project Structure - But kept flat for now to run in Databricks


├── notebooks/          # Databricks notebooks for data processing
│   └── ecom_process_job.py
├── tests/              # Unit tests for data processing functions
│   ├── script/
│   │   └── test_job.py
│   ├── test_data/
│   │   ├── Input_test_data/
│   │   │   ├── order.json
│   │   │   ├── customer.csv
│   │   │   └── product.csv
│   │   └── Expected_data/
│   │       ├── expected_customer.csv
│   │       ├── expected_product.csv
│   │       └── expected_comprehensive_order.csv
├── docs/               # Documentation and data dictionaries
├── Sources/            # Raw source data files
│   ├── Customer.xlsx
│   ├── Orders.json
│   └── Products.csv

## Technologies Used

- Databricks
- PySpark
- Delta Lake
- Pandas (for Excel processing)

## Data Sources

- Products data (CSV)
- Customers data (Excel)
- Orders data (JSON)

## Key Features

- Automated data quality scoring
- Customer and product data enrichment
- Comprehensive order analytics with joins
- Profit aggregation by various dimensions
- Unit tests for data processing functions

## Usage

Run the notebook cells in sequence to process the data pipeline. Ensure the required volumes and schemas are created before execution.

## Tests

The notebook includes unit tests for data enrichment and aggregation functions.
