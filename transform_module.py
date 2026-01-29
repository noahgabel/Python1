"""
Transform Module - ETL Pipeline
This module uses PySpark to transform and filter data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


def transform_iris_data(input_csv_path):
    """
    Transform iris data using PySpark:
    - Load CSV data
    - Filter to only include Iris-setosa species
    - Return filtered DataFrame

    Args:
        input_csv_path: Path to the input CSV file

    Returns:
        PySpark DataFrame containing only Iris-setosa observations
    """

    # Initialize Spark session
    # master("local[*]") runs locally using all available cores
    spark = SparkSession.builder \
        .appName("IrisETL") \
        .master("local[*]") \
        .getOrCreate()

    # Suppress excessive logging
    spark.sparkContext.setLogLevel("WARN")

    # Define schema for iris dataset
    # Based on data science documentation:
    # sepal_length, sepal_width, petal_length, petal_width, species
    schema = StructType([
        StructField("sepal_length", DoubleType(), True),
        StructField("sepal_width", DoubleType(), True),
        StructField("petal_length", DoubleType(), True),
        StructField("petal_width", DoubleType(), True),
        StructField("species", StringType(), True)
    ])

    # Load CSV data with PySpark
    df = spark.read.csv(
        input_csv_path,
        header=False,  # Original file has no header
        schema=schema
    )

    print(f"Original data count: {df.count()} rows")

    # Transform: Filter only Iris-setosa species
    # PySpark uses lazy evaluation - filter is not executed until an action is called
    df_filtered = df.filter(df.species == "Iris-setosa")

    print(f"Filtered data count: {df_filtered.count()} rows (Iris-setosa only)")

    return df_filtered, spark
