"""
Transform Module (Pandas version) - ETL Pipeline
This module uses Pandas to transform and filter data.
Alternative to PySpark for Windows compatibility.
"""

import pandas as pd


def transform_iris_data_pandas(input_csv_path):
    """
    Transform iris data using Pandas:
    - Load CSV data
    - Filter to only include Iris-setosa species
    - Return filtered DataFrame

    Args:
        input_csv_path: Path to the input CSV file

    Returns:
        Pandas DataFrame containing only Iris-setosa observations
    """

    # Define column names for iris dataset
    # Based on data science documentation:
    # sepal_length, sepal_width, petal_length, petal_width, species
    column_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']

    # Load CSV data with Pandas
    df = pd.read_csv(input_csv_path, names=column_names, header=None)

    print(f"Original data count: {len(df)} rows")

    # Transform: Filter only Iris-setosa species
    df_filtered = df[df['species'] == 'Iris-setosa'].copy()

    print(f"Filtered data count: {len(df_filtered)} rows (Iris-setosa only)")

    return df_filtered
