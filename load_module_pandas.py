"""
Load Module - ETL Pipeline
"""

import os
import pymysql
from urllib.parse import urlparse


def load_to_csv_pandas(df_pandas, output_folder, original_url):
    """
    Save transformed data to CSV file.
    """
    parsed_url = urlparse(original_url)
    original_filename = os.path.basename(parsed_url.path)

    base_name = os.path.splitext(original_filename)[0]
    extension = os.path.splitext(original_filename)[1]
    new_filename = f"transformed_{base_name}{extension}"

    destination_path = os.path.join(output_folder, new_filename)

    os.makedirs(output_folder, exist_ok=True)

    df_pandas.to_csv(destination_path, index=False)

    print(f"[OK] Saved to CSV: {destination_path}")
    return destination_path


def load_to_mysql_pandas(df_pandas, db_config):
    """
    Save transformed data to MySQL database.
    Creates database and table if they don't exist.
    """
    host = db_config.get('host', 'localhost')
    user = db_config.get('user', 'root')
    password = db_config.get('password', '')
    database = db_config.get('database', 'iris_db')
    table = db_config.get('table', 'iris_setosa')

    # Create database if not exists
    connection = pymysql.connect(host=host, user=user, password=password)
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    print(f"[OK] Database '{database}' ready")
    cursor.close()
    connection.close()

    # Save data to table
    connection = pymysql.connect(host=host, user=user, password=password, database=database)

    try:
        cursor = connection.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS {table}")

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
    Read data from MySQL database.
    """
    import pandas as pd

    host = db_config.get('host', 'localhost')
    user = db_config.get('user', 'root')
    password = db_config.get('password', '')
    database = db_config.get('database', 'iris_db')
    table = db_config.get('table', 'iris_setosa')

    connection = pymysql.connect(host=host, user=user, password=password, database=database)

    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, connection)
    connection.close()

    print(f"[OK] Read {len(df)} rows from MySQL: {database}.{table}")
    return df
