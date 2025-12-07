"""Data transformation and enrichment functions."""

import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, regexp_replace,
    length, trim, lpad, udf, row_number, to_date, year, sum as spark_sum
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


def enrich_product_data(raw_products_df: DataFrame) -> DataFrame:
    """
    Clean and enrich products data.
    
    Assumptions:
    1. Product ID should not be null
    2. Generate data_quality_score based on completeness of category, sub-category, and product name
    3. Create price_range category based on price per product
    4. Clean state names by removing special characters
    5. Round price per product to 2 decimal places
    6. Generate is_available flag for products with valid prices
    
    Args:
        raw_products_df: Raw products DataFrame
        
    Returns:
        Enriched products DataFrame
    """
    enriched_products_df = raw_products_df \
        .withColumn("price_per_product", spark_round(col("price_per_product"), 2)) \
        .withColumn("price_range",
                when(col("price_per_product") < 10, "Budget")
                .when(col("price_per_product") < 50, "Standard")
                .when(col("price_per_product") < 200, "Premium")
                .otherwise("Luxury")) \
        .withColumn("State",
                regexp_replace(col("State"), "[^A-Za-z\\s]", "")) \
        .withColumn("is_available",
                when(col("price_per_product").isNotNull() &
                     (col("price_per_product") > 0), True).otherwise(False)) \
        .withColumn("data_quality_score",
                (when(col("Product_Name").isNotNull(), 1).otherwise(0) +
                 when(col("Category").isNotNull(), 1).otherwise(0) +
                 when(col("price_per_product").isNotNull(), 1).otherwise(0)) / 3.0) \
        .filter(col("Product_id").isNotNull())
    
    # Drop duplicates based on Product_ID, keeping the entry with higher price
    w = Window.partitionBy("Product_ID").orderBy(col("price_per_product").desc())
    
    enriched_products_df = (
        enriched_products_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    return enriched_products_df


def enrich_customer_data(raw_customers_df: DataFrame) -> DataFrame:
    """
    Enrich customer data by cleaning customer names and phone numbers.
    
    Transformations:
    1. Customer name: Remove multiple whitespaces and special characters
    2. Phone number: Must be at least 10 digits, otherwise set to null
    3. Postal code: Pad to 5 digits with leading zeros
    
    Args:
        raw_customers_df: Raw customers DataFrame
        
    Returns:
        Enriched customers DataFrame
    """
    def clean_customer_name(name):
        """Clean customer name by removing invalid characters and formatting."""
        if not name or not isinstance(name, str):
            return None
        
        # Remove leading/trailing whitespace and special characters except apostrophe
        name = name.strip()
        name = re.sub(r"^[^A-Za-z']+|[^A-Za-z']+$", '', name)
        
        # Remove embedded special characters, digits, and multiple spaces except apostrophe
        name = re.sub(r"[^A-Za-z'\s]", '', name)
        name = re.sub(r'\s+', ' ', name)
        
        # Remove single-letter fragments at start (e.g., "B ecky Martin")
        name = re.sub(r'^[A-Za-z]\s+', '', name)
        name = re.sub(r"^[^\w]+|[^\w]+$", "", name)
        
        # Capitalize each word, preserving apostrophes
        name = ' '.join([w.capitalize() for w in name.split()])
        
        return name if name else None
    
    clean_customer_name_udf = udf(clean_customer_name, StringType())
    
    cleaned_customers_df = raw_customers_df.withColumn(
        "customer_name_clean",
        clean_customer_name_udf(trim(col("Customer_name")))
    ).withColumn(
        "clean_phone",
        when((col("phone").rlike("ERROR!")) | (length("phone") < 10), lit(None))
        .otherwise(regexp_replace("phone", "[^0-9x()]", ""))
    ).withColumn(
        "postal_code",
        lpad(col("postal_code").cast("string"), 5, "0")
    ).drop("Customer_name", "phone") \
    .withColumnRenamed("customer_name_clean", "Customer_name") \
    .withColumnRenamed("clean_phone", "phone")
    
    # Filter out records where customer name is null and remove duplicates
    cleaned_customers_df = cleaned_customers_df \
        .filter(col("Customer_name").isNotNull()) \
        .dropDuplicates(["Customer_ID", "customer_name"])
    
    return cleaned_customers_df


def comprehensive_order_data(
    cleaned_customers_df: DataFrame,
    raw_orders_df: DataFrame,
    enriched_products_df: DataFrame
) -> DataFrame:
    """
    Create a comprehensive order table by joining customer, order, and product data.
    
    Args:
        cleaned_customers_df: Cleaned customers DataFrame
        raw_orders_df: Raw orders DataFrame
        enriched_products_df: Enriched products DataFrame
        
    Returns:
        Comprehensive orders DataFrame with all joins and calculated fields
    """
    cust_df = cleaned_customers_df.alias('cust')
    orders_df = raw_orders_df.alias('order')
    prod_df = enriched_products_df.alias('prod')
    
    logger.info(f"Orders columns: {orders_df.columns}")
    
    comprehensive_orders_df = orders_df \
        .join(
            cust_df,
            orders_df["Customer_ID"] == cust_df["Customer_ID"],
            "left"
        ) \
        .join(
            prod_df,
            orders_df["Product_ID"] == prod_df["Product_ID"],
            "left"
        ) \
        .drop(cust_df["Customer_ID"], prod_df["Product_ID"]) \
        .select(
            'order.*',
            "cust.Customer_name",
            "cust.Country",
            "prod.product_name",
            "prod.category",
            "prod.sub_category"
        )
    
    comprehensive_orders_df = comprehensive_orders_df \
        .withColumn("profit", spark_round(col("profit"), 2)) \
        .withColumn("Order_Date", to_date(col("Order_Date"), "d/M/yyyy")) \
        .withColumn("year", year(col("Order_Date")))
    
    return comprehensive_orders_df


def create_profit_aggregates(comprehensive_orders_df: DataFrame) -> DataFrame:
    """
    Create comprehensive profit aggregates table grouped by multiple dimensions.
    
    Args:
        comprehensive_orders_df: Comprehensive orders DataFrame
        
    Returns:
        Aggregated profit DataFrame
    """
    try:
        aggregates_df = comprehensive_orders_df.groupBy(
            "year",
            "Category",
            "Sub_Category",
            "Customer_id",
            "customer_name"
        ).agg(
            spark_round(spark_sum("profit"), 2).alias("total_profit")
        )
        
        return aggregates_df
    except Exception as e:
        logger.error(f"Error creating profit aggregates: {str(e)}")
        raise e
