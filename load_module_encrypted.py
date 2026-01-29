"""
Load Module - ETL Pipeline (Encrypted Version)
Med AES kryptering af data
"""

import os
import pymysql
import pandas as pd
from urllib.parse import urlparse
from security_module import SecurityManager


def load_to_csv_pandas_encrypted(df_pandas, output_folder, original_url, security_mgr):
    """
    Gem transformeret data til CSV fil - KRYPTERET

    Krypterer alle numeriske kolonner med valgt AES metode.
    """
    parsed_url = urlparse(original_url)
    original_filename = os.path.basename(parsed_url.path)

    base_name = os.path.splitext(original_filename)[0]
    extension = os.path.splitext(original_filename)[1]
    new_filename = f"transformed_{base_name}_encrypted{extension}"

    destination_path = os.path.join(output_folder, new_filename)
    os.makedirs(output_folder, exist_ok=True)

    # Lav en kopi af dataframe til kryptering
    df_encrypted = df_pandas.copy()

    # Krypter numeriske kolonner
    numeric_columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']

    for col in numeric_columns:
        df_encrypted[col] = df_encrypted[col].apply(
            lambda x: security_mgr.encrypt_aes_gcm(str(x))
        )

    df_encrypted.to_csv(destination_path, index=False)

    print(f"[OK] Saved ENCRYPTED data to CSV: {destination_path}")
    return destination_path


def load_to_mysql_pandas_encrypted(df_pandas, db_config, security_mgr):
    """
    Gem transformeret data til MySQL database - KRYPTERET

    Krypterer alle numeriske kolonner før database insert.
    """
    host = db_config.get('host', 'localhost')
    user = db_config.get('user', 'root')
    password = db_config.get('password', '')
    database = db_config.get('database', 'iris_db')
    table = db_config.get('table', 'iris_setosa_encrypted')

    # Create database
    connection = pymysql.connect(host=host, user=user, password=password)
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    print(f"[OK] Database '{database}' ready")
    cursor.close()
    connection.close()

    # Save encrypted data
    connection = pymysql.connect(host=host, user=user, password=password, database=database)

    try:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

        # Opdateret tabel struktur til TEXT (krypterede værdier er længere)
        create_table_sql = f"""
        CREATE TABLE {table} (
            sepal_length TEXT,
            sepal_width TEXT,
            petal_length TEXT,
            petal_width TEXT,
            species VARCHAR(50)
        )
        """
        cursor.execute(create_table_sql)

        # Krypter og insert data
        for _, row in df_pandas.iterrows():
            encrypted_row = (
                security_mgr.encrypt_aes_gcm(str(row['sepal_length'])),
                security_mgr.encrypt_aes_gcm(str(row['sepal_width'])),
                security_mgr.encrypt_aes_gcm(str(row['petal_length'])),
                security_mgr.encrypt_aes_gcm(str(row['petal_width'])),
                row['species']
            )

            insert_sql = f"""
            INSERT INTO {table}
            (sepal_length, sepal_width, petal_length, petal_width, species)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, encrypted_row)

        connection.commit()
        print(f"[OK] Saved ENCRYPTED data to MySQL: {database}.{table}")

    finally:
        cursor.close()
        connection.close()


def read_from_mysql_encrypted(db_config, security_mgr):
    """
    Læs data fra MySQL database og DEKRYPTER

    Dekrypterer alle numeriske kolonner efter læsning fra database.
    """
    host = db_config.get('host', 'localhost')
    user = db_config.get('user', 'root')
    password = db_config.get('password', '')
    database = db_config.get('database', 'iris_db')
    table = db_config.get('table', 'iris_setosa_encrypted')

    connection = pymysql.connect(host=host, user=user, password=password, database=database)

    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, connection)
    connection.close()

    print(f"[OK] Read {len(df)} encrypted rows from MySQL: {database}.{table}")

    # Dekrypter numeriske kolonner
    numeric_columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']

    for col in numeric_columns:
        df[col] = df[col].apply(
            lambda x: float(security_mgr.decrypt_aes_gcm(x))
        )

    print(f"[OK] Decrypted {len(df)} rows")

    return df


def read_from_csv_encrypted(csv_path, security_mgr):
    """
    Læs krypteret CSV fil og DEKRYPTER

    Dekrypterer alle numeriske kolonner.
    """
    df = pd.read_csv(csv_path)

    print(f"[OK] Read {len(df)} encrypted rows from CSV: {csv_path}")

    # Dekrypter numeriske kolonner
    numeric_columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']

    for col in numeric_columns:
        df[col] = df[col].apply(
            lambda x: float(security_mgr.decrypt_aes_gcm(x))
        )

    print(f"[OK] Decrypted {len(df)} rows")

    return df
