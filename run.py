"""
Main entry point for running the e-commerce data processing pipeline.

Usage:
    python run.py [--setup] [--reports]
    
Options:
    --setup     Run environment setup first
    --reports   Generate SQL reports after pipeline
"""

import sys
import logging
from pyspark.sql import SparkSession
%pip install openpyxl pandas
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main execution function."""
    # Parse command line arguments
    setup_env = '--setup' in sys.argv
    generate_reports = '--reports' in sys.argv
    
    # Initialize Spark session
    logger.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("Ecommerce Data Processing") \
        .getOrCreate()
    
    # Setup environment if requested
    if setup_env:
        logger.info("Setting up environment...")
        from setup_environment import setup_environment
        setup_environment(spark)
    
    # Run the pipeline
    logger.info("Starting data pipeline...")
    from src.pipeline import run_pipeline
    run_pipeline(spark)
    
    # Generate reports if requested
    if generate_reports:
        logger.info("Generating SQL reports...")
        from src.pipeline import generate_sql_reports
        generate_sql_reports(spark)
    
    logger.info("Execution completed successfully!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        sys.exit(1)
