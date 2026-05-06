"""
Delta Table Merge with Temporary View and Variable Join Columns
This script demonstrates how to create a temporary view from a dataframe
and use it in a Delta table merge statement with flexible join conditions.
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

def create_merge_statement_with_temp_view(spark, source_df, target_table_path, join_columns, temp_view_name="source_temp_view"):
    """
    Creates a temporary view from source dataframe and performs Delta merge operation.
    
    Args:
        spark: SparkSession object
        source_df: Source DataFrame to merge from
        target_table_path: Path to the target Delta table
        join_columns: List of column names to join on
        temp_view_name: Name for the temporary view (default: "source_temp_view")
    """
    
    # Step 1: Create temporary view by selecting all columns from dataframe
    source_df.createOrReplaceTempView(temp_view_name)
    
    # Step 2: Build the merge condition dynamically based on join columns
    merge_conditions = []
    for col in join_columns:
        merge_conditions.append(f"target.{col} = source.{col}")
    
    merge_condition = " AND ".join(merge_conditions)
    
    # Step 3: Create the merge SQL statement using the temporary view
    merge_sql = f"""
    MERGE INTO delta.`{target_table_path}` AS target
    USING {temp_view_name} AS source
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    
    # Step 4: Execute the merge statement
    spark.sql(merge_sql)
    
    return merge_sql

def create_merge_with_delta_api(spark, source_df, target_table_path, join_columns, temp_view_name="source_temp_view"):
    """
    Alternative approach using Delta Table API instead of SQL.
    """
    
    # Create temporary view
    source_df.createOrReplaceTempView(temp_view_name)
    
    # Load the target Delta table
    delta_table = DeltaTable.forPath(spark, target_table_path)
    
    # Build merge condition for Delta API
    merge_conditions = []
    for col in join_columns:
        merge_conditions.append(f"target.{col} = source.{col}")
    
    merge_condition = " AND ".join(merge_conditions)
    
    # Execute merge using Delta API
    delta_table.alias("target").merge(
        spark.table(temp_view_name).alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Example usage
if __name__ == "__main__":
    # Initialize Spark session with Delta support
    spark = SparkSession.builder \
        .appName("DeltaMergeWithTempView") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Example: Create sample source dataframe
    source_data = [
        (1, "John", "Doe", 25, "2024-01-01"),
        (2, "Jane", "Smith", 30, "2024-01-02"),
        (3, "Bob", "Johnson", 35, "2024-01-03")
    ]
    
    columns = ["id", "first_name", "last_name", "age", "date_created"]
    source_df = spark.createDataFrame(source_data, columns)
    
    # Define target table path and join columns
    target_table_path = "/path/to/your/delta/table"
    join_columns = ["id"]  # Can be multiple columns: ["id", "first_name"]
    
    # Method 1: Using SQL with temporary view
    print("Creating temporary view and executing merge...")
    merge_sql = create_merge_statement_with_temp_view(
        spark, 
        source_df, 
        target_table_path, 
        join_columns, 
        "employees_temp"
    )
    
    print(f"Generated merge SQL:\n{merge_sql}")
    
    # Method 2: Using Delta Table API (alternative)
    # create_merge_with_delta_api(spark, source_df, target_table_path, join_columns)
    
    # Clean up
    spark.catalog.dropTempView("employees_temp")
    spark.stop()

# Additional examples for different scenarios:

def merge_with_custom_conditions(spark, source_df, target_table_path, join_columns, 
                                 update_columns=None, insert_columns=None):
    """
    More advanced merge with custom update and insert conditions.
    """
    temp_view_name = "custom_merge_view"
    source_df.createOrReplaceTempView(temp_view_name)
    
    # Build join condition
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in join_columns])
    
    # Build update clause
    if update_columns:
        update_set = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
        update_clause = f"UPDATE SET {update_set}"
    else:
        update_clause = "UPDATE SET *"
    
    # Build insert clause
    if insert_columns:
        insert_values = ", ".join([f"source.{col}" for col in insert_columns])
        insert_clause = f"INSERT ({', '.join(insert_columns)}) VALUES ({insert_values})"
    else:
        insert_clause = "INSERT *"
    
    merge_sql = f"""
    MERGE INTO delta.`{target_table_path}` AS target
    USING {temp_view_name} AS source
    ON {merge_condition}
    WHEN MATCHED THEN {update_clause}
    WHEN NOT MATCHED THEN {insert_clause}
    """
    
    spark.sql(merge_sql)
    return merge_sql

def merge_with_multiple_join_columns_example():
    """
    Example showing merge with multiple join columns.
    """
    # Example with multiple join columns
    join_columns = ["customer_id", "product_id", "date"]
    
    # This would create a merge condition like:
    # target.customer_id = source.customer_id AND 
    # target.product_id = source.product_id AND 
    # target.date = source.date
    
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in join_columns])
    print(f"Multi-column join condition: {merge_condition}")