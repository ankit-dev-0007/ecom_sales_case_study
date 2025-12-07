# E-commerce Data Processing Case Study

This repository contains a modular data processing pipeline for e-commerce sales data using PySpark and Delta Lake. The project demonstrates a data lakehouse architecture with bronze, silver, and gold layers for data ingestion, processing, and aggregation.

## Overview

The pipeline implements an end-to-end data processing solution for e-commerce data, including:

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

## Project Structure

```
case_study_pei/
├── src/
│   ├── __init__.py
│   ├── config.py              # Configuration settings
│   ├── utils.py               # Utility functions (sanitize, save)
│   ├── ingestion.py           # Data loading functions
│   ├── transformations.py     # Data enrichment and transformation
│   └── pipeline.py            # Main pipeline orchestration
├── tests/
│   ├── __init__.py
│   ├── conftest.py            # Pytest configuration and fixtures
│   ├── test_data_factory.py  # Test data creation
│   └── test_transformations.py # Unit tests for transformations
├── ecom_process_job_db_notebook.ipynb  # Original notebook
├── requirements.txt           # Python dependencies
├── pytest.ini                 # Pytest configuration
├── .gitignore
└── README.md
```

## Technologies Used

- **Databricks**: Cloud-based data platform
- **PySpark**: Distributed data processing
- **Delta Lake**: ACID transactions and time travel
- **Pandas**: Excel file processing
- **pytest**: Unit testing framework

## Data Sources

- Products data (CSV)
- Customers data (Excel)
- Orders data (JSON)

## Key Features

- Modular, maintainable code structure
- Automated data quality scoring
- Customer and product data enrichment
- Comprehensive order analytics with joins
- Profit aggregation by various dimensions
- Comprehensive unit tests with pytest

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Running the Pipeline

To run the complete data pipeline:

```python
from pyspark.sql import SparkSession
from src.pipeline import run_pipeline, generate_sql_reports

spark = SparkSession.builder \
    .appName("Ecommerce Data Processing") \
    .getOrCreate()

# Run the pipeline
run_pipeline(spark)

# Generate reports
generate_sql_reports(spark)
```

### Running Individual Modules

```python
from src.ingestion import load_products_data
from src.transformations import enrich_product_data

# Load data
raw_products_df = load_products_data(spark)

# Enrich data
enriched_products_df = enrich_product_data(raw_products_df)
```

## Testing

Run all tests using pytest:

```bash
pytest
```

Run specific test file:

```bash
pytest tests/test_transformations.py
```

Run with verbose output:

```bash
pytest -v
```

## Tests

The test suite includes:

- **test_customer_enrichment**: Validates customer name cleaning and phone number formatting
- **test_product_enrichment**: Validates product data enrichment, price ranges, and quality scores
- **test_order_enrichment**: Validates order data joins and date processing
- **test_aggregate**: Validates profit aggregation and rounding

## Data Quality Rules

### Customer Data
- Customer name must not be null (records filtered out)
- Phone numbers must be at least 10 digits
- Postal codes padded to 5 digits
- Special characters removed from names

### Product Data
- Product ID must not be null
- Duplicate Product IDs resolved by keeping highest price
- Price ranges categorized (Budget, Standard, Premium, Luxury)
- Data quality score calculated based on field completeness

### Order Data
- Dates parsed and year extracted
- Profits rounded to 2 decimal places
- Left joins preserve all order data

## Contributing

1. Create a new branch for your feature
2. Write tests for new functionality
3. Ensure all tests pass
4. Submit a pull request
