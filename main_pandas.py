"""
Main ETL Pipeline Script (Pandas Version - Windows Compatible)
Pipeline med ETL, database og datavisualisering

This script orchestrates the complete ETL pipeline using Pandas instead of PySpark.
This version is fully Windows-compatible without requiring Hadoop installation.

1. Extract: Download iris data from remote source
2. Transform: Filter only Iris-setosa species using Pandas
3. Load: Save to CSV and MySQL database
4. Visualize: Create scatter plot, histogram, and boxplots
"""

import extract_module
import transform_module_pandas
import load_module_pandas
import visualization_module


def main():
    """
    Main function to execute the ETL pipeline.
    """

    print("=" * 70)
    print("ETL PIPELINE - IRIS DATA ANALYSIS (Pandas Version)")
    print("=" * 70)

    # Configuration
    DATA_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
    INPUT_FOLDER = "Input_dir"
    OUTPUT_FOLDER = "Output_dir"

    # MySQL configuration
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': '',  # Update with your MySQL password
        'database': 'iris_db',
        'table': 'iris_setosa'
    }

    # =========================================================================
    # STEP 1: EXTRACT
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 1: EXTRACT - Downloading data")
    print("=" * 70)

    """
    Security and Robustness Reflection:

    Three methods are available for downloading:
    1. requests library
    2. wget library
    3. subprocess + curl

    CHOSEN METHOD: requests (Method 1)

    Why this method is best:

    SECURITY:
    - No command-line injection risk: Pure Python library, no shell execution
    - HTTPS support: Automatically handles secure connections
    - No external dependencies: Doesn't rely on system binaries
    - Certificate validation: Validates SSL certificates by default

    ROBUSTNESS:
    - Streaming downloads: Uses iter_content() to download in chunks
    - Timeout control: Can specify connection and read timeouts
    - Error handling: Proper exception handling with raise_for_status()
    - Cross-platform: Works on Windows, Linux, macOS without modifications

    RISKS WITH OTHER METHODS:

    Method 2 (wget):
    - Less control: Limited fine-tuning of download behavior
    - External dependency: Requires wget package installation
    - Less common: Not as widely used as requests in Python

    Method 3 (subprocess + curl):
    - Command injection risk: Even with list form, adds complexity
    - Platform dependency: curl may not be available on all Windows systems
    - Harder to test: Requires mocking subprocess calls
    - Less Pythonic: Relies on external system commands
    - Error handling: More difficult to parse curl error messages

    CONCLUSION:
    The requests library provides the best balance of security, robustness,
    and cross-platform compatibility for this ETL pipeline.
    """

    # Download using the chosen method: requests
    try:
        downloaded_file = extract_module.extract_with_requests(DATA_URL, INPUT_FOLDER)

        # Uncomment below to test other methods:
        # downloaded_file = extract_module.extract_with_wget(DATA_URL, INPUT_FOLDER)
        # downloaded_file = extract_module.extract_with_subprocess_curl(DATA_URL, INPUT_FOLDER)

    except Exception as e:
        print(f"[ERROR] Extract failed: {e}")
        return

    # =========================================================================
    # STEP 2: TRANSFORM
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 2: TRANSFORM - Filtering Iris-setosa data with Pandas")
    print("=" * 70)

    try:
        df_filtered = transform_module_pandas.transform_iris_data_pandas(downloaded_file)
    except Exception as e:
        print(f"[ERROR] Transform failed: {e}")
        return

    # =========================================================================
    # STEP 3: LOAD
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 3: LOAD - Saving transformed data")
    print("=" * 70)

    # Load to CSV
    try:
        csv_output = load_module_pandas.load_to_csv_pandas(df_filtered, OUTPUT_FOLDER, DATA_URL)
    except Exception as e:
        print(f"[ERROR] Load to CSV failed: {e}")
        return

    # Load to MySQL
    try:
        load_module_pandas.load_to_mysql_pandas(df_filtered, DB_CONFIG)
    except Exception as e:
        print(f"[WARNING] Load to MySQL failed: {e}")
        print("Continuing without MySQL database...")

    # =========================================================================
    # STEP 4: VISUALIZE
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 4: VISUALIZE - Creating data visualizations")
    print("=" * 70)

    # Read data from MySQL for visualization
    try:
        df_for_viz = load_module_pandas.read_from_mysql(DB_CONFIG)
    except Exception as e:
        print(f"[WARNING] Could not read from MySQL: {e}")
        print("Using CSV file for visualization instead...")

        # Fallback: Read from CSV
        import pandas as pd
        df_for_viz = pd.read_csv(csv_output)

    # Create visualizations
    print("\nGenerating visualizations...")
    print("(Close each plot window to proceed to the next)")

    # Scatter plot
    visualization_module.create_scatter_plot(df_for_viz)

    # Histogram
    visualization_module.create_histogram(df_for_viz)

    # Boxplots
    visualization_module.create_boxplots(df_for_viz)

    # =========================================================================
    # PIPELINE COMPLETE
    # =========================================================================
    print("\n" + "=" * 70)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"\nSummary:")
    print(f"  - Data extracted from: {DATA_URL}")
    print(f"  - Original data saved to: {INPUT_FOLDER}")
    print(f"  - Transformed data (Iris-setosa only) saved to:")
    print(f"    * CSV: {csv_output}")
    try:
        print(f"    * MySQL: {DB_CONFIG['database']}.{DB_CONFIG['table']}")
    except:
        print(f"    * MySQL: (not configured)")
    print(f"  - Visualizations: scatter plot, histogram, boxplots")
    print(f"  - Total filtered records: {len(df_for_viz)}")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
