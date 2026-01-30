"""
Main ETL Pipeline Script - MED KRYPTERING
"""

import extract_module
import transform_module_pandas
import load_module_encrypted
import visualization_module
from security_module import SecurityManager, save_key, load_key


def main():
    print("=" * 70)
    print("ETL PIPELINE - IRIS DATA ANALYSIS (MED KRYPTERING)")
    print("=" * 70)

    DATA_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
    INPUT_FOLDER = "Input_dir"
    OUTPUT_FOLDER = "Output_dir"

    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': 'pjotr1234',
        'database': 'iris_db',
        'table': 'iris_setosa_encrypted'
    }

    # =========================================================================
    # KRYPTERINGS SETUP
    # =========================================================================
    print("\n" + "=" * 70)
    print("KRYPTERINGS SETUP")
    print("=" * 70)

    """
    VALG AF KRYPTERINGSMETODE: AES-GCM

    3 implementerede metoder:
    1. AES-GCM
    2. AES-CBC
    3. Fernet

    VALGT: AES-GCM

    Begrundelse:

    AES-GCM er bedst til iris data fordi:
    - AEAD (integritet og autenticitet indbygget)
    - Hurtigst (hardware acceleration)
    - Ingen padding nødvendig
    - Beskytter mod manipulation
    - Moderne standard (anbefalet til nye systemer)

    Hvorfor IKKE de andre:
    - AES-CBC: Kræver HMAC separat, padding, langsommere
    - Fernet: Større overhead, langsommere

    Konklusion: AES-GCM giver bedste balance for vores data.
    """

    # Indlæs eller generer nøgle
    key = load_key('encryption.key')
    if key is None:
        security_mgr = SecurityManager()
        save_key(security_mgr.get_key(), 'encryption.key')
    else:
        security_mgr = SecurityManager(key)

    print(f"[OK] Using AES-GCM encryption")

    # EXTRACT
    print("\n" + "=" * 70)
    print("STEP 1: EXTRACT")
    print("=" * 70)

    try:
        downloaded_file = extract_module.extract_with_requests(DATA_URL, INPUT_FOLDER)
    except Exception as e:
        print(f"[ERROR] Extract failed: {e}")
        return

    # TRANSFORM
    print("\n" + "=" * 70)
    print("STEP 2: TRANSFORM")
    print("=" * 70)

    try:
        df_filtered = transform_module_pandas.transform_iris_data_pandas(downloaded_file)
    except Exception as e:
        print(f"[ERROR] Transform failed: {e}")
        return

    # LOAD (KRYPTERET)
    print("\n" + "=" * 70)
    print("STEP 3: LOAD - ENCRYPTED")
    print("=" * 70)

    try:
        csv_output = load_module_encrypted.load_to_csv_pandas_encrypted(
            df_filtered, OUTPUT_FOLDER, DATA_URL, security_mgr
        )
    except Exception as e:
        print(f"[ERROR] Load to CSV failed: {e}")
        return

    try:
        load_module_encrypted.load_to_mysql_pandas_encrypted(
            df_filtered, DB_CONFIG, security_mgr
        )
    except Exception as e:
        print(f"[WARNING] Load to MySQL failed: {e}")
        print("Continuing without MySQL...")

    # VISUALIZE (DEKRYPTERET)
    print("\n" + "=" * 70)
    print("STEP 4: VISUALIZE - DECRYPTED")
    print("=" * 70)

    try:
        df_for_viz = load_module_encrypted.read_from_mysql_encrypted(DB_CONFIG, security_mgr)
    except Exception as e:
        print(f"[WARNING] Could not read from MySQL: {e}")
        print("Using CSV file...")
        df_for_viz = load_module_encrypted.read_from_csv_encrypted(csv_output, security_mgr)

    print("\nGenerating visualizations...")
    print("(Close each plot window to proceed)")

    visualization_module.create_scatter_plot(df_for_viz)
    visualization_module.create_histogram(df_for_viz)
    visualization_module.create_boxplots(df_for_viz)

    # SUMMARY
    print("\n" + "=" * 70)
    print("ENCRYPTED ETL PIPELINE COMPLETED!")
    print("=" * 70)
    print(f"\nSummary:")
    print(f"  - Data source: {DATA_URL}")
    print(f"  - Encrypted CSV: {csv_output}")
    print(f"  - Encrypted MySQL: {DB_CONFIG['database']}.{DB_CONFIG['table']}")
    print(f"  - Encryption: AES-GCM")
    print(f"  - Records: {len(df_for_viz)}")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
