"""Main data pipeline orchestration."""

import logging
from pyspark.sql import SparkSession
from src.ingestion import load_products_data, load_customers_data, load_orders_data
from src.transformations import (
    enrich_product_data,
    enrich_customer_data,
    comprehensive_order_data,
    create_profit_aggregates
)
from src.utils import save_as_delta_table
from src.config import SILVER_DELTA_PATH, GOLD_DELTA_PATH, CATALOG_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline(spark: SparkSession):
    """
    Execute the complete data processing pipeline.
    
    Pipeline stages:
    1. Ingest raw data (Bronze layer)
    2. Enrich and transform data (Silver layer)
    3. Create aggregations (Gold layer)
    
    Args:
        spark: Active Spark session
    """
    logger.info("Starting data pipeline...")
    
    # Stage 1: Ingest raw data
    logger.info("Stage 1: Ingesting raw data...")
    raw_products_df = load_products_data(spark)
    raw_customers_df = load_customers_data(spark)
    raw_orders_df = load_orders_data(spark)
    
    # Stage 2: Enrich and transform data
    logger.info("Stage 2: Enriching data...")
    enriched_products_df = enrich_product_data(raw_products_df)
    save_as_delta_table(
        enriched_products_df,
        "products_processed",
        SILVER_DELTA_PATH,
        "processed_layer",
        CATALOG_NAME
    )
    
    cleaned_customers_df = enrich_customer_data(raw_customers_df)
    save_as_delta_table(
        cleaned_customers_df,
        "customer_processed",
        SILVER_DELTA_PATH,
        "processed_layer",
        CATALOG_NAME
    )
    
    comprehensive_orders_df = comprehensive_order_data(
        cleaned_customers_df,
        raw_orders_df,
        enriched_products_df
    )
    save_as_delta_table(
        comprehensive_orders_df,
        "comprehensive_orders",
        SILVER_DELTA_PATH,
        "processed_layer",
        CATALOG_NAME
    )
    
    # Stage 3: Create aggregations
    logger.info("Stage 3: Creating aggregations...")
    aggregate_df = create_profit_aggregates(comprehensive_orders_df)
    save_as_delta_table(
        aggregate_df,
        "profit_aggregates",
        GOLD_DELTA_PATH,
        "gold_layer",
        CATALOG_NAME
    )
    
    logger.info("Pipeline completed successfully!")


def generate_sql_reports(spark: SparkSession):
    """
    Generate SQL-based business reports.
    
    Args:
        spark: Active Spark session
    """
    try:
        logger.info("=== Profit by Year ===")
        profit_by_year_df = spark.sql("""
            SELECT
                year,
                ROUND(SUM(profit), 2) as total_profit
            FROM ecommerce_sales.processed_layer.comprehensive_orders
            GROUP BY year
            ORDER BY year
        """)
        profit_by_year_df.show()

        logger.info("=== Profit by Year + Product Category ===")
        profit_by_year_category_df = spark.sql("""
            SELECT
                year,
                Category,
                ROUND(SUM(profit), 2) as total_profit
            FROM ecommerce_sales.processed_layer.comprehensive_orders
            GROUP BY year, Category
            ORDER BY year, Category
        """)
        profit_by_year_category_df.show()

        logger.info("=== Profit by Customer ===")
        profit_by_customer_df = spark.sql("""
            SELECT
                customer_id, 
                customer_name,
                ROUND(SUM(profit), 2) as total_profit
            FROM ecommerce_sales.processed_layer.comprehensive_orders
            GROUP BY customer_id, customer_name
            ORDER BY total_profit DESC
        """)
        profit_by_customer_df.show()

        logger.info("=== Profit by Customer + Year ===")
        profit_by_customer_year_df = spark.sql("""
            SELECT
                customer_id, 
                customer_name,
                year,
                ROUND(SUM(profit), 2) as total_profit
            FROM ecommerce_sales.processed_layer.comprehensive_orders
            GROUP BY customer_id, customer_name, year
            ORDER BY customer_name, year
        """)
        profit_by_customer_year_df.show()

    except Exception as e:
        logger.error(f"Error generating SQL reports: {str(e)}")
        raise


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Ecommerce Data Processing") \
        .getOrCreate()
    
    run_pipeline(spark)
    generate_sql_reports(spark)
