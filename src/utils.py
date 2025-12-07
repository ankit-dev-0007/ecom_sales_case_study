"""Utility functions for data processing."""

import re
import logging
from pyspark.sql import DataFrame

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sanitize_column(input_df: DataFrame) -> DataFrame:
    """
    Standardize column names to be compatible with Delta/Parquet standards.
    Remove spaces and special characters from column names.
    
    Args:
        input_df: Input DataFrame with potentially non-standard column names
        
    Returns:
        DataFrame with sanitized column names
    """
    for c in input_df.columns:
        new_name = re.sub(r"[ ]", "_", c)
        new_name = re.sub(r"[-]", "_", new_name)
        new_name = re.sub(r"[,;{}()\n\t=*]", "", new_name)
        input_df = input_df.withColumnRenamed(c, new_name)
    
    return input_df


def save_as_delta_table(
    df: DataFrame,
    table_name: str,
    table_path: str,
    schema_nm: str,
    catalog: str = "ecommerce_sales"
) -> bool:
    """
    Save DataFrame as Delta table with error handling.
    
    Args:
        df: DataFrame to save
        table_name: Name of the table
        table_path: Base path for the Delta table
        schema_nm: Schema name
        catalog: Catalog name (default: "ecommerce_sales")
        
    Returns:
        True if successful, False otherwise
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        # Save as Delta table
        full_table_path = f"{table_path}/{table_name}"
        logger.info(f"Saving to path: {full_table_path}")
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(full_table_path)

        # Create table in metastore
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema_nm}.{table_name}
            USING DELTA AS
            SELECT * FROM delta.`{full_table_path}`
        """)

        logger.info(f"Successfully saved {table_name} as Delta table")
        return True
    except Exception as e:
        logger.error(f"Failed to save {table_name} as Delta table: {str(e)}")
        return False
