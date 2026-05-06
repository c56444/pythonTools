"""
PySpark Script: Read Parquet File into Temporary View
This script demonstrates how to read a parquet file and create a temporary view from it.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def read_parquet_to_temp_view(spark, parquet_file_path, view_name):
    """
    Reads a parquet file and creates a temporary view.
    
    Args:
        spark: SparkSession object
        parquet_file_path: Path to the parquet file
        view_name: Name for the temporary view
    
    Returns:
        DataFrame: The loaded DataFrame
    """
    
    # Read parquet file into DataFrame
    df = spark.read.parquet(parquet_file_path)
    
    # Create temporary view
    df.createOrReplaceTempView(view_name)
    
    print(f"Successfully created temporary view '{view_name}' from parquet file: {parquet_file_path}")
    print(f"Schema of the view:")
    df.printSchema()
    
    return df

def read_multiple_parquet_files_to_view(spark, parquet_paths, view_name):
    """
    Reads multiple parquet files and creates a single temporary view.
    
    Args:
        spark: SparkSession object
        parquet_paths: List of paths to parquet files or a path pattern
        view_name: Name for the temporary view
    
    Returns:
        DataFrame: The combined DataFrame
    """
    
    # Read multiple parquet files
    df = spark.read.parquet(*parquet_paths)
    
    # Create temporary view
    df.createOrReplaceTempView(view_name)
    
    print(f"Successfully created temporary view '{view_name}' from {len(parquet_paths)} parquet files")
    print(f"Total rows: {df.count()}")
    
    return df

def read_parquet_with_options_to_view(spark, parquet_file_path, view_name, **options):
    """
    Reads a parquet file with custom options and creates a temporary view.
    
    Args:
        spark: SparkSession object
        parquet_file_path: Path to the parquet file
        view_name: Name for the temporary view
        **options: Additional read options
    
    Returns:
        DataFrame: The loaded DataFrame
    """
    
    # Read parquet with options
    df_reader = spark.read.options(**options)
    df = df_reader.parquet(parquet_file_path)
    
    # Create temporary view
    df.createOrReplaceTempView(view_name)
    
    print(f"Created temporary view '{view_name}' with options: {options}")
    
    return df

def query_temp_view(spark, view_name, query=None):
    """
    Execute a query against the temporary view.
    
    Args:
        spark: SparkSession object
        view_name: Name of the temporary view
        query: Optional SQL query (defaults to SELECT *)
    
    Returns:
        DataFrame: Query result
    """
    
    if query is None:
        query = f"SELECT * FROM {view_name}"
    
    result = spark.sql(query)
    return result

# Example usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ParquetToTempView") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Example 1: Basic parquet file to temporary view
    parquet_file_path = "/path/to/your/data.parquet"
    view_name = "data_view"
    
    try:
        # Read parquet and create temp view
        df = read_parquet_to_temp_view(spark, parquet_file_path, view_name)
        
        # Show sample data from the view
        print("\nSample data from temporary view:")
        spark.sql(f"SELECT * FROM {view_name} LIMIT 10").show()
        
        # Get row count
        row_count = spark.sql(f"SELECT COUNT(*) as total_rows FROM {view_name}").collect()[0]['total_rows']
        print(f"\nTotal rows in view: {row_count}")
        
        # Example queries using the temporary view
        print("\nExample queries:")
        
        # Simple aggregation
        agg_result = spark.sql(f"""
            SELECT COUNT(*) as row_count, 
                   COUNT(DISTINCT *) as distinct_rows 
            FROM {view_name}
        """)
        agg_result.show()
        
        # Column information
        columns_info = spark.sql(f"DESCRIBE {view_name}")
        columns_info.show()
        
    except Exception as e:
        print(f"Error reading parquet file: {str(e)}")
        
        # Create sample data for demonstration
        print("\nCreating sample data for demonstration...")
        sample_data = [
            (1, "John", "Doe", 25, 50000.0),
            (2, "Jane", "Smith", 30, 60000.0),
            (3, "Bob", "Johnson", 35, 70000.0),
            (4, "Alice", "Williams", 28, 55000.0)
        ]
        
        columns = ["id", "first_name", "last_name", "age", "salary"]
        sample_df = spark.createDataFrame(sample_data, columns)
        
        # Create temporary view from sample data
        sample_df.createOrReplaceTempView("sample_view")
        
        print("Sample temporary view created. Showing data:")
        spark.sql("SELECT * FROM sample_view").show()
        
        # Save as parquet for future use
        sample_parquet_path = "./sample_data.parquet"
        sample_df.write.mode("overwrite").parquet(sample_parquet_path)
        print(f"Sample data saved to: {sample_parquet_path}")
    
    # Example 2: Reading multiple parquet files
    print("\n--- Example 2: Multiple Parquet Files ---")
    parquet_paths = [
        "/path/to/data/year=2023/*.parquet",
        "/path/to/data/year=2024/*.parquet"
    ]
    
    # Uncomment to test with actual files
    # multi_df = read_multiple_parquet_files_to_view(spark, parquet_paths, "multi_year_view")
    
    # Example 3: Reading with custom options
    print("\n--- Example 3: Parquet with Options ---")
    options = {
        "mergeSchema": "true",
        "pathGlobFilter": "*.parquet"
    }
    
    # Uncomment to test with actual files
    # options_df = read_parquet_with_options_to_view(
    #     spark, 
    #     "/path/to/data/", 
    #     "options_view", 
    #     **options
    # )
    
    # Example 4: Advanced queries on temporary view
    print("\n--- Example 4: Advanced Queries ---")
    
    # Create a more complex sample view for demonstration
    complex_data = [
        (1, "Sales", "John", 50000, "2024-01-01"),
        (2, "Marketing", "Jane", 60000, "2024-01-15"),
        (3, "Sales", "Bob", 55000, "2024-02-01"),
        (4, "IT", "Alice", 70000, "2024-02-15")
    ]
    
    complex_columns = ["id", "department", "name", "salary", "hire_date"]
    complex_df = spark.createDataFrame(complex_data, complex_columns)
    complex_df.createOrReplaceTempView("employees")
    
    # Department-wise analysis
    dept_analysis = spark.sql("""
        SELECT department,
               COUNT(*) as employee_count,
               AVG(salary) as avg_salary,
               MAX(salary) as max_salary,
               MIN(salary) as min_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """)
    
    print("Department-wise salary analysis:")
    dept_analysis.show()
    
    # Date-based filtering
    recent_hires = spark.sql("""
        SELECT name, department, salary, hire_date
        FROM employees
        WHERE hire_date >= '2024-02-01'
        ORDER BY hire_date DESC
    """)
    
    print("Recent hires (Feb 2024 onwards):")
    recent_hires.show()
    
    # Clean up - drop temporary views
    spark.catalog.dropTempView("sample_view")
    spark.catalog.dropTempView("employees")
    
    # List all temporary views (should be empty now)
    temp_views = spark.catalog.listTables()
    print(f"\nRemaining temporary views: {[view.name for view in temp_views if view.tableType == 'TEMPORARY']}")
    
    spark.stop()

# Utility functions for common operations

def list_temp_views(spark):
    """List all temporary views in the current session."""
    views = spark.catalog.listTables()
    temp_views = [view.name for view in views if view.tableType == 'TEMPORARY']
    return temp_views

def drop_temp_view_if_exists(spark, view_name):
    """Drop a temporary view if it exists."""
    try:
        spark.catalog.dropTempView(view_name)
        print(f"Dropped temporary view: {view_name}")
        return True
    except Exception:
        print(f"Temporary view '{view_name}' does not exist")
        return False

def get_temp_view_schema(spark, view_name):
    """Get schema information for a temporary view."""
    try:
        df = spark.table(view_name)
        return df.schema
    except Exception as e:
        print(f"Error getting schema for view '{view_name}': {str(e)}")
        return None