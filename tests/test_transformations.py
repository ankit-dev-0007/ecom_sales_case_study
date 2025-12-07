"""Tests for data transformation functions."""

import pytest
from pyspark.sql.functions import col
from src.transformations import (
    enrich_customer_data,
    enrich_product_data,
    comprehensive_order_data,
    create_profit_aggregates
)
from tests.test_data_factory import create_test_data


def test_customer_enrichment(spark):
    """Test customer data enrichment."""
    customers_df, _, _ = create_test_data(spark)
    
    expected_cust_op = [
        (
            "CUST001", "john.doe@gmail.com", "new jersey,usa", "Consumer",
            "USA", "New York", "NY", "10001", "East", "John Doe", None
        ),
        (
            "CUST002", "john.doe@gmail.com", "boston,usa", "Corporate",
            "Canada", "Toronto", "ON", "00M5V", "East", "Jane Smith", "9811144367"
        )
    ]
    
    col_list = [
        "Customer_ID", "email", "address", "segment",
        "Country", "City", "State", "Postal_Code", "Region", "Customer_Name", "phone"
    ]
    
    expected_cust_df = spark.createDataFrame(expected_cust_op, col_list)
    
    # Add enrichment fields
    enriched_df = enrich_customer_data(customers_df)
    
    # Verify Count - Null customer_name record is removed
    assert enriched_df.count() == 2, "Record count didn't match"
    
    # Compare actual & expected data
    # 1. Records with null customer_name will not be available
    # 2. Pincode will be standardized to 5 digits by adding leading zeros
    # 3. Phone number with length less than 10 will be marked as null
    assert expected_cust_df.collect() == enriched_df.collect(), "Records didn't match"


def test_product_enrichment(spark):
    """Test product data enrichment."""
    _, products_df, _ = create_test_data(spark)
    
    expected_product_op = [
        ("PROD001", "Technology", "Phones", "iPhone 12", "CA", 999.99,
         "Luxury", True, 1.0),
        ("PROD002", "Furniture", "Chairs", "Office Chair", "NY", 299.99,
         "Luxury", True, 1.0),
        ("PROD003", "Office Supplies", "Paper", "Printer Paper", "TX", 20.0,
         "Standard", True, 1.0)
    ]
    
    col_list = [
        "product_id", "category", "sub_category", "product_name", "state",
        "price_per_product", "price_range", "is_available", "data_quality_score"
    ]
    
    expected_product_df = spark.createDataFrame(expected_product_op, col_list)
    
    # Add enrichment fields
    enriched_df = enrich_product_data(products_df)
    
    # Verify enrichment
    assert "price_range" in enriched_df.columns
    assert "is_available" in enriched_df.columns
    
    # Check price ranges
    budget_count = enriched_df.filter(col("price_range") == "Standard").count()
    luxury_count = enriched_df.filter(col("price_range") == "Luxury").count()
    assert budget_count == 1  # Printer paper
    assert luxury_count == 2  # iPhone, office chair
    assert enriched_df.count() == 3, "Record count didn't match"
    
    assert expected_product_df.collect() == enriched_df.collect(), "Records didn't match"


def test_order_enrichment(spark):
    """Test order data enrichment with joins."""
    customers_df, products_df, orders_df = create_test_data(spark)
    enriched_cust_df = enrich_customer_data(customers_df)
    enriched_prod_df = enrich_product_data(products_df)
    
    # Perform joins
    enriched_orders = comprehensive_order_data(
        enriched_cust_df, orders_df, enriched_prod_df
    )
    
    # Verify joins and calculations
    assert enriched_orders.count() == 3
    enrich_order_cols = [x.lower() for x in enriched_orders.columns]
    assert "customer_name" in enrich_order_cols
    assert "category" in enrich_order_cols
    assert "sub_category" in enrich_order_cols
    assert "year" in enrich_order_cols


def test_aggregate(spark):
    """Test profit aggregation."""
    customers_df, products_df, orders_df = create_test_data(spark)
    enriched_cust_df = enrich_customer_data(customers_df)
    enriched_prod_df = enrich_product_data(products_df)
    
    # Perform joins
    enriched_orders = comprehensive_order_data(
        enriched_cust_df, orders_df, enriched_prod_df
    )
    
    aggregates_df = create_profit_aggregates(enriched_orders)
    
    assert aggregates_df.count() > 0
    
    # Check that profits are rounded to 2 decimal places
    profit_values = aggregates_df.select("total_profit").collect()
    for row in profit_values:
        profit_str = str(row["total_profit"])
        if '.' in profit_str:
            decimal_places = len(profit_str.split('.')[1])
            assert decimal_places <= 2, \
                f"Aggregated profit should be rounded to 2 decimal places, got {decimal_places}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
