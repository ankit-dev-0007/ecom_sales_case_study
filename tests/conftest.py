"""Pytest configuration and fixtures."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    spark_session = SparkSession.builder \
        .appName("TestDataEnrichment") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark_session
    
    spark_session.stop()
