"""
Main ETL Pipeline Script - MED KRYPTERING
Pipeline med ETL, database, datavisualisering og AES kryptering
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

    Jeg har implementeret 3 krypteringsmetoder:
    1. AES-GCM (Galois/Counter Mode)
    2. AES-CBC (Cipher Block Chaining)
    3. Fernet (AES-CBC wrapper)

    VALGT METODE: AES-GCM

    Begrundelse for valg af AES-GCM til iris datatype:

    For iris blomster data (numeriske målinger + art navn) er AES-GCM bedst fordi:

    SIKKERHED:
    - AEAD (Authenticated Encryption): Indbygget integritet og autenticitet
    - Beskytter mod manipulation af krypteret data
    - Detekterer hvis nogen ændrer de krypterede værdier

    PERFORMANCE:
    - Hurtigst af de 3 metoder (hardware acceleration)
    - Vigtig når vi krypterer 50+ rækker med 4 kolonner hver

    SIMPLICITET:
    - Ingen padding nødvendig (modsat AES-CBC)
    - Mindre kompleks end Fernet
    - Færre fejlmuligheder

    MODERNE STANDARD:
    - Anbefalet til nye systemer
    - Bruges i TLS 1.3, moderne API'er

    Hvorfor IKKE de andre metoder:

    AES-CBC:
    - Kræver separat HMAC for integritet
    - Kræver padding (PKCS7) - ekstra kompleksitet
    - Risiko for padding oracle attacks
    - Langsommere end GCM

    Fernet:
    - Større overhead (længere ciphertext)
    - Langsommere end GCM
    - Mindre fleksibel
    - Bedre til simple use cases, men overkill her

    KONKLUSION: AES-GCM giver bedste balance mellem sikkerhed,
    performance og simplicitet for vores blomster data.
    """

    # Indlæs eller generer krypteringsnøgle
    key = load_key('encryption.key')
    if key is None:
        security_mgr = SecurityManager()
        save_key(security_mgr.get_key(), 'encryption.key')
    else:
        security_mgr = SecurityManager(key)

    print(f"[OK] Using AES-GCM encryption")

    # =========================================================================
    # STEP 1: EXTRACT
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 1: EXTRACT - Downloading data")
    print("=" * 70)

    try:
        downloaded_file = extract_module.extract_with_requests(DATA_URL, INPUT_FOLDER)
    except Exception as e:
        print(f"[ERROR] Extract failed: {e}")
        return

    # =========================================================================
    # STEP 2: TRANSFORM
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 2: TRANSFORM - Filtering Iris-setosa data")
    print("=" * 70)

    try:
        df_filtered = transform_module_pandas.transform_iris_data_pandas(downloaded_file)
    except Exception as e:
        print(f"[ERROR] Transform failed: {e}")
        return

    # =========================================================================
    # STEP 3: LOAD (MED KRYPTERING)
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 3: LOAD - Saving ENCRYPTED transformed data")
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

    # =========================================================================
    # STEP 4: VISUALIZE (MED DEKRYPTERING)
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 4: VISUALIZE - DECRYPTING and visualizing data")
    print("=" * 70)

    try:
        df_for_viz = load_module_encrypted.read_from_mysql_encrypted(DB_CONFIG, security_mgr)
    except Exception as e:
        print(f"[WARNING] Could not read from MySQL: {e}")
        print("Using CSV file for visualization...")
        df_for_viz = load_module_encrypted.read_from_csv_encrypted(csv_output, security_mgr)

    print("\nGenerating visualizations...")
    print("(Close each plot window to proceed to the next)")

    visualization_module.create_scatter_plot(df_for_viz)
    visualization_module.create_histogram(df_for_viz)
    visualization_module.create_boxplots(df_for_viz)

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("ENCRYPTED ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"\nSummary:")
    print(f"  - Data extracted from: {DATA_URL}")
    print(f"  - Original data: {INPUT_FOLDER}")
    print(f"  - Encrypted data saved to:")
    print(f"    * CSV: {csv_output}")
    print(f"    * MySQL: {DB_CONFIG['database']}.{DB_CONFIG['table']}")
    print(f"  - Encryption method: AES-GCM")
    print(f"  - Total filtered records: {len(df_for_viz)}")
    print(f"  - Visualizations: scatter plot, histogram, boxplots")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
