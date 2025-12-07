"""Setup script for initializing the Databricks environment."""

from pyspark.sql import SparkSession


def create_catalog_and_schemas(spark: SparkSession):
    """Create the catalog and schemas for the data pipeline."""
    
    # Create catalog
    spark.sql("""
        CREATE CATALOG IF NOT EXISTS ecommerce_sales 
        COMMENT 'Ecommerce sales data processing'
    """)
    
    # Create schemas
    spark.sql("USE CATALOG ecommerce_sales")
    
    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS source_data_layer 
        COMMENT 'Source data'
    """)
    
    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS bronze_layer 
        COMMENT 'Raw data'
    """)
    
    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS processed_layer 
        COMMENT 'Processed data'
    """)
    
    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS gold_layer 
        COMMENT 'Final publish layer - Aggregated data'
    """)
    
    print("✓ Catalog and schemas created successfully")


def create_volumes(spark: SparkSession):
    """Create volumes for storing data files."""
    
    spark.sql("USE CATALOG ecommerce_sales")
    
    # Bronze layer volume
    spark.sql("USE SCHEMA bronze_layer")
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS ecom_volume_raw 
        COMMENT 'Managed volume for raw files'
    """)
    
    # Silver layer volume
    spark.sql("USE SCHEMA processed_layer")
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS ecom_volume_processed 
        COMMENT 'Managed volume for processed files'
    """)
    
    # Gold layer volume
    spark.sql("USE SCHEMA gold_layer")
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS ecom_volume_gold 
        COMMENT 'Managed volume for final publish aggregated files'
    """)
    
    print("✓ Volumes created successfully")


def setup_environment(spark: SparkSession):
    """Run complete environment setup."""
    print("Setting up Databricks environment...")
    create_catalog_and_schemas(spark)
    create_volumes(spark)
    print("✓ Environment setup complete!")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Setup Ecommerce Environment") \
        .getOrCreate()
    
    setup_environment(spark)
