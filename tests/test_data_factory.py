"""Test data factory for creating sample test data."""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


def create_test_data(spark: SparkSession):
    """
    Create test data for unit testing.
    
    Returns:
        Tuple of (customers_df, products_df, orders_df)
    """
    # Test customer data
    customer_data = [
        (
            "CUST001", "John Doe", "john.doe@gmail.com", "-1874",
            "new jersey,usa", "Consumer", "USA", "New York", "NY", "10001", "East"
        ),
        (
            "CUST002", "Jane.  ..Smith", "john.doe@gmail.com", "9811144367",
            "boston,usa", "Corporate", "Canada", "Toronto", "ON", "M5V", "East"
        ),
        (
            "CUST003", None, "abc@gmail.com", "(91)x876678990",
            "101, bob hueston road, Washington", "Home Office",
            "USA", "Chicago", "IL", "60601", "Central"
        )
    ]

    customer_schema = StructType([
        StructField("Customer_ID", StringType(), True),
        StructField("Customer_Name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal_Code", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    customers_df = spark.createDataFrame(customer_data, customer_schema)

    # Test product data
    product_data = [
        ("PROD001", "Technology", "Phones", "iPhone 12", "CA", 999.99),
        ("PROD002", "Furniture", "Chairs", "Office Chair", "NY", 299.99),
        ("PROD003", "Office Supplies", "Paper", "Printer Paper", "TX", 19.998),
        ("PROD003", "Admin", "Account", "Printer", "BO", 9.99),
        (None, "Furniture", "Table", "Office Table", "NJ", 499.99)
    ]

    product_schema = StructType([
        StructField("Product_ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub_Category", StringType(), True),
        StructField("Product_Name", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Price_per_product", DoubleType(), True)
    ])

    products_df = spark.createDataFrame(product_data, product_schema)

    # Test order data
    order_data = [
        (1, "ORD001", "15/1/2023", "20/1/2023", "Standard Class",
         "CUST001", "PROD001", 2, 1999.98, 0.1, 199.99),
        (2, "ORD002", "16/1/2023", "21/1/2023", "First Class",
         "CUST002", "PROD002", 1, 299.99, 0.0, 89.99),
        (3, "ORD003", "17/1/2023", "22/1/2023", "Second Class",
         "CUST001", "PROD003", 5, 49.95, 0.2, 9.99)
    ]

    order_schema = StructType([
        StructField("Row_ID", IntegerType(), True),
        StructField("Order_ID", StringType(), True),
        StructField("Order_Date", StringType(), True),
        StructField("Ship_Date", StringType(), True),
        StructField("Ship_Mode", StringType(), True),
        StructField("Customer_ID", StringType(), True),
        StructField("Product_ID", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True)
    ])

    orders_df = spark.createDataFrame(order_data, order_schema)

    return customers_df, products_df, orders_df
