"""
Load Module (Pandas version) - ETL Pipeline
This module saves transformed Pandas data to CSV and MySQL database.
Alternative to PySpark for Windows compatibility.
"""

import os
import pymysql
from urllib.parse import urlparse


def load_to_csv_pandas(df_pandas, output_folder, original_url):
    """
    Save transformed Pandas DataFrame to CSV file.

    Args:
        df_pandas: Pandas DataFrame to save
        output_folder: Destination folder for CSV file
        original_url: Original download URL (to extract filename)

    Returns:
        Path to saved CSV file
    """

    # Extract original filename from URL
    parsed_url = urlparse(original_url)
    original_filename = os.path.basename(parsed_url.path)

    # Create new filename with "transformed_" prefix
    base_name = os.path.splitext(original_filename)[0]
    extension = os.path.splitext(original_filename)[1]
    new_filename = f"transformed_{base_name}{extension}"

    destination_path = os.path.join(output_folder, new_filename)

    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Save to CSV with headers
    df_pandas.to_csv(destination_path, index=False)

    print(f"[OK] Saved to CSV: {destination_path}")
    return destination_path


def load_to_mysql_pandas(df_pandas, db_config):
    """
    Save transformed Pandas DataFrame to MySQL database.

    Args:
        df_pandas: Pandas DataFrame to save
        db_config: Dictionary with MySQL connection parameters:
                   - host: MySQL host
                   - user: MySQL username
                   - password: MySQL password
                   - database: Database name
                   - table: Table name

    The function will:
    - Create database if it doesn't exist
    - Overwrite table data on each run
    """

    host = db_config.get('host', 'localhost')
    user = db_config.get('user', 'root')
    password = db_config.get('password', '')
    database = db_config.get('database', 'iris_db')
    table = db_config.get('table', 'iris_setosa')

    # First, create database if it doesn't exist
    try:
        # Connect without specifying database
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        # Create database if not exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        print(f"[OK] Database '{database}' ready")

        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        raise

    # Now save DataFrame to MySQL
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    try:
        cursor = connection.cursor()

        # Drop table if exists (to overwrite)
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

        # Create table
        create_table_sql = f"""
        CREATE TABLE {table} (
            sepal_length DOUBLE,
            sepal_width DOUBLE,
            petal_length DOUBLE,
            petal_width DOUBLE,
            species VARCHAR(50)
        )
        """
        cursor.execute(create_table_sql)

        # Insert data
        for _, row in df_pandas.iterrows():
            insert_sql = f"""
            INSERT INTO {table}
            (sepal_length, sepal_width, petal_length, petal_width, species)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, tuple(row))

        connection.commit()
        print(f"[OK] Saved to MySQL: {database}.{table}")

    finally:
        cursor.close()
        connection.close()


def read_from_mysql(db_config):
    """
    Read data from MySQL database and return as Pandas DataFrame.

    Args:
        db_config: Dictionary with MySQL connection parameters

    Returns:
        Pandas DataFrame with data from MySQL
    """
    import pandas as pd

    host = db_config.get('host', 'localhost')
    user = db_config.get('user', 'root')
    password = db_config.get('password', '')
    database = db_config.get('database', 'iris_db')
    table = db_config.get('table', 'iris_setosa')

    # Connect and read data
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, connection)
    connection.close()

    print(f"[OK] Read {len(df)} rows from MySQL: {database}.{table}")
    return df
