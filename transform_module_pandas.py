"""
Transform Module - ETL Pipeline
"""

import pandas as pd


def transform_iris_data_pandas(input_csv_path):
    """
    Transform iris data using Pandas - filter only Iris-setosa species.
    """
    column_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']

    df = pd.read_csv(input_csv_path, names=column_names, header=None)

    print(f"Original data count: {len(df)} rows")

    df_filtered = df[df['species'] == 'Iris-setosa'].copy()

    print(f"Filtered data count: {len(df_filtered)} rows (Iris-setosa only)")

    return df_filtered
