"""
Main ETL Pipeline Script
Pipeline med ETL, database og datavisualisering
"""

import extract_module
import transform_module_pandas
import load_module_pandas
import visualization_module


def main():
    print("=" * 70)
    print("ETL PIPELINE - IRIS DATA ANALYSIS")
    print("=" * 70)

    DATA_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
    INPUT_FOLDER = "Input_dir"
    OUTPUT_FOLDER = "Output_dir"

    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': 'pjotr1234',
        'database': 'iris_db',
        'table': 'iris_setosa'
    }

    # EXTRACT
    print("\n" + "=" * 70)
    print("STEP 1: EXTRACT - Downloading data")
    print("=" * 70)

    """
    Sikkerhedsvurdering:

    Valgt metode: requests (Method 1)

    Hvorfor denne metode er bedst:
    - SIKKERHED: Ingen command-line injection risiko (pure Python library)
    - SIKKERHED: HTTPS support er indbygget
    - ROBUSTHED: Streaming download i chunks håndterer store filer
    - ROBUSTHED: Timeout forhindrer hængende connections

    Risici ved andre metoder:
    - wget: Mindre kontrol over download process
    - subprocess+curl: Command injection risiko selv med list form,
                       curl er ikke altid tilgængelig på Windows
    """

    try:
        downloaded_file = extract_module.extract_with_requests(DATA_URL, INPUT_FOLDER)
    except Exception as e:
        print(f"[ERROR] Extract failed: {e}")
        return

    # TRANSFORM
    print("\n" + "=" * 70)
    print("STEP 2: TRANSFORM - Filtering Iris-setosa data")
    print("=" * 70)

    try:
        df_filtered = transform_module_pandas.transform_iris_data_pandas(downloaded_file)
    except Exception as e:
        print(f"[ERROR] Transform failed: {e}")
        return

    # LOAD
    print("\n" + "=" * 70)
    print("STEP 3: LOAD - Saving transformed data")
    print("=" * 70)

    try:
        csv_output = load_module_pandas.load_to_csv_pandas(df_filtered, OUTPUT_FOLDER, DATA_URL)
    except Exception as e:
        print(f"[ERROR] Load to CSV failed: {e}")
        return

    try:
        load_module_pandas.load_to_mysql_pandas(df_filtered, DB_CONFIG)
    except Exception as e:
        print(f"[WARNING] Load to MySQL failed: {e}")
        print("Continuing without MySQL...")

    # VISUALIZE
    print("\n" + "=" * 70)
    print("STEP 4: VISUALIZE - Creating data visualizations")
    print("=" * 70)

    try:
        df_for_viz = load_module_pandas.read_from_mysql(DB_CONFIG)
    except Exception as e:
        print(f"[WARNING] Could not read from MySQL: {e}")
        print("Using CSV file for visualization...")
        import pandas as pd
        df_for_viz = pd.read_csv(csv_output)

    print("\nGenerating visualizations...")
    print("(Close each plot window to proceed to the next)")

    visualization_module.create_scatter_plot(df_for_viz)
    visualization_module.create_histogram(df_for_viz)
    visualization_module.create_boxplots(df_for_viz)

    # SUMMARY
    print("\n" + "=" * 70)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"\nSummary:")
    print(f"  - Data extracted from: {DATA_URL}")
    print(f"  - Original data: {INPUT_FOLDER}")
    print(f"  - Transformed data: {csv_output}")
    print(f"  - Total filtered records: {len(df_for_viz)}")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
