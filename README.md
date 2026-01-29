# ETL Pipeline - Iris Data Analysis

## Project Overview
This is a Python ETL (Extract-Transform-Load) pipeline for analyzing Iris flower data. The pipeline downloads data from a remote source, filters it using PySpark, stores it in MySQL database, and creates visualizations.

## Project Structure
```
pythonProject/
│
├── main.py                    # Main orchestration script
├── extract_module.py          # Data extraction with 3 methods
├── transform_module.py        # Data transformation using PySpark
├── load_module.py             # Data loading to CSV and MySQL
├── visualization_module.py    # Data visualization with matplotlib
├── requirements.txt           # Python dependencies
│
├── Input_dir/                 # Downloaded raw data
└── Output_dir/                # Transformed data output
```

## Requirements

### Python Version
- Python 3.8 or higher

### Dependencies
Install all dependencies with:
```bash
pip install -r requirements.txt
```

Required packages:
- `requests` - For HTTP data extraction
- `wget` - Alternative data extraction method
- `pyspark` - For data transformation
- `pymysql` - For MySQL database connection
- `pandas` - For data manipulation
- `matplotlib` - For data visualization

### System Requirements
- **MySQL Server**: Install MySQL and ensure it's running
- **Java**: PySpark requires Java 8 or later
- **curl** (optional): For testing subprocess extraction method

## Configuration

### MySQL Setup
1. Install MySQL server
2. Update MySQL credentials in `main.py`:
```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'your_password',  # Update this
    'database': 'iris_db',
    'table': 'iris_setosa'
}
```

The database and table will be created automatically if they don't exist.

## Usage

### Running the Pipeline
```bash
python main.py
```

### Pipeline Steps
The script will execute:

1. **EXTRACT**: Download iris.csv from GitHub
   - Uses the `requests` library (recommended)
   - Alternative methods: wget, subprocess+curl
   - Data saved to `Input_dir/iris.csv`

2. **TRANSFORM**: Filter data using PySpark
   - Filters only Iris-setosa species
   - Uses PySpark DataFrame operations

3. **LOAD**: Save transformed data
   - CSV file: `Output_dir/transformed_iris.csv`
   - MySQL: `iris_db.iris_setosa` table

4. **VISUALIZE**: Generate plots
   - Scatter plot: sepal_length vs petal_length
   - Histogram: petal_width distribution
   - Boxplots: All 4 measurements (2×2 layout)

## Module Details

### Extract Module (`extract_module.py`)
Provides 3 extraction methods:

1. **extract_with_requests()** (Recommended)
   - Secure HTTPS support
   - No command injection risk
   - Cross-platform compatible
   - Streaming downloads

2. **extract_with_wget()**
   - Python wget library
   - Built-in retry mechanisms

3. **extract_with_subprocess_curl()**
   - Uses system curl command
   - Demonstrates safe subprocess usage

### Transform Module (`transform_module.py`)
- Uses PySpark for data transformation
- Filters data to include only Iris-setosa species
- Defines schema: sepal_length, sepal_width, petal_length, petal_width, species

### Load Module (`load_module.py`)
Two loading methods:
1. **load_to_csv()**: Saves to CSV with column headers
2. **load_to_mysql()**: Saves to MySQL database
   - Auto-creates database if missing
   - Overwrites table on each run
   - Falls back to Pandas if JDBC driver unavailable

### Visualization Module (`visualization_module.py`)
Three visualization functions:
1. **create_scatter_plot()**: sepal_length vs petal_length
2. **create_histogram()**: petal_width distribution (10 bins)
3. **create_boxplots()**: 2×2 layout of all measurements

## Security Considerations

### Command-Line Injection Prevention
- **requests method**: Pure Python, no shell execution
- **wget method**: Python library, no shell commands
- **subprocess method**: Uses list form (`subprocess.run(['curl', url])`) to prevent injection

### HTTPS Support
All extraction methods support HTTPS for secure data transport.

### Robustness
- Streaming downloads handle large files
- Error handling at each pipeline stage
- Automatic retry mechanisms (where supported)
- Timeout controls

## Testing Different Extraction Methods

To test alternative extraction methods, edit `main.py`:

```python
# Method 1: requests (default)
downloaded_file = extract_module.extract_with_requests(DATA_URL, INPUT_FOLDER)

# Method 2: wget
# downloaded_file = extract_module.extract_with_wget(DATA_URL, INPUT_FOLDER)

# Method 3: subprocess + curl
# downloaded_file = extract_module.extract_with_subprocess_curl(DATA_URL, INPUT_FOLDER)
```

## Data Schema

The Iris dataset contains:
- `sepal_length`: Sepal length in cm (bægerblad længde)
- `sepal_width`: Sepal width in cm (bægerblad bredde)
- `petal_length`: Petal length in cm (kronblad længde)
- `petal_width`: Petal width in cm (kronblad bredde)
- `species`: Iris species (Iris-setosa, Iris-versicolor, Iris-virginica)

This pipeline filters only **Iris-setosa** observations.

## Troubleshooting

### PySpark Issues
If PySpark fails to start:
- Ensure Java 8+ is installed: `java -version`
- Set JAVA_HOME environment variable

### MySQL Connection Issues
- Verify MySQL is running: `systemctl status mysql` (Linux) or check Services (Windows)
- Check credentials in `main.py`
- Ensure MySQL server allows local connections

### curl Not Found (Windows)
- curl may not be available on older Windows versions
- Use requests or wget methods instead

### JDBC Driver Not Found
- The script will automatically fall back to Pandas+pymysql
- For full JDBC support, download MySQL Connector/J JAR

## Assignment Compliance

This implementation fulfills all assignment requirements:

✓ **Faglige mål 1**: ETL-programmeringsmønster implemented
✓ **Faglige mål 2**: Custom Python modules (extract, transform, load, visualization)
✓ **Faglige mål 3**: Secure and robust data extraction from internet sources
✓ **Faglige mål 4**: PySpark for data processing
✓ **Faglige mål 6**: SQL database integration
✓ **Faglige mål 7**: Data visualization with matplotlib

## Author
Python II Assignment - Data Processing Pipeline
