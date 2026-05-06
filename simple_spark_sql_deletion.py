#!/usr/bin/env python3
"""
Simple Spark DataFrame to SQL Deletion Examples

Basic patterns for deleting SQL table rows using Apache Spark DataFrame values.
Perfect for distributed data processing and large-scale deletions.

Required packages:
    pip install pyspark pandas sqlalchemy

Usage:
    python simple_spark_sql_deletion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
import pandas as pd
import sqlite3
import os


def create_spark_session():
    """Create and configure Spark session."""
    spark = SparkSession.builder \
        .appName("SimpleSQLDeletion") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def setup_sample_database():
    """Create sample SQLite database and table."""
    conn = sqlite3.connect('spark_sample.db')
    cursor = conn.cursor()
    
    # Create employees table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            department TEXT,
            salary REAL,
            active BOOLEAN DEFAULT 1
        )
    ''')
    
    # Insert sample data
    employees = [
        (1, 'Alice Johnson', 'alice@company.com', 'Engineering', 95000, 1),
        (2, 'Bob Smith', 'bob@company.com', 'Marketing', 72000, 1),
        (3, 'Carol Davis', 'carol@company.com', 'Finance', 78000, 1),
        (4, 'David Wilson', 'david@company.com', 'Engineering', 92000, 1),
        (5, 'Eve Brown', 'eve@company.com', 'HR', 68000, 1),
        (6, 'Frank Miller', 'frank@company.com', 'Marketing', 75000, 0),
        (7, 'Grace Lee', 'grace@company.com', 'Finance', 81000, 1),
        (8, 'Henry Garcia', 'henry@company.com', 'Engineering', 88000, 1),
        (9, 'Iris Chen', 'iris@company.com', 'HR', 70000, 1),
        (10, 'Jack Taylor', 'jack@company.com', 'Finance', 79000, 1),
        (11, 'Kate Williams', 'kate@company.com', 'Engineering', 97000, 1),
        (12, 'Liam Jones', 'liam@company.com', 'Marketing', 73000, 1)
    ]
    
    cursor.executemany('''
        INSERT OR REPLACE INTO employees 
        (id, name, email, department, salary, active) 
        VALUES (?, ?, ?, ?, ?, ?)
    ''', employees)
    
    conn.commit()
    conn.close()
    print("✅ Sample database created")


def show_current_data():
    """Display current database contents."""
    conn = sqlite3.connect('spark_sample.db')
    df = pd.read_sql_query("SELECT * FROM employees ORDER BY id", conn)
    conn.close()
    
    print(f"\n📊 Current Employee Data ({len(df)} records):")
    print(df.to_string(index=False))
    return df


def example_1_delete_by_spark_ids():
    """Example 1: Delete employees by IDs from Spark DataFrame."""
    
    print("\n" + "="*70)
    print("EXAMPLE 1: DELETE BY IDs FROM SPARK DATAFRAME")
    print("="*70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Create Spark DataFrame with employee IDs to delete
    ids_data = [(2,), (4,), (6,), (8,)]
    ids_schema = StructType([StructField("employee_id", IntegerType(), True)])
    ids_df = spark.createDataFrame(ids_data, ids_schema)
    
    print("Spark DataFrame with IDs to delete:")
    ids_df.show()
    
    # Collect IDs from Spark DataFrame (small dataset, safe to collect)
    ids_to_delete = [row.employee_id for row in ids_df.collect()]
    print(f"IDs to delete: {ids_to_delete}")
    
    # Delete from SQL database
    conn = sqlite3.connect('spark_sample.db')
    placeholders = ','.join(['?' for _ in ids_to_delete])
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", ids_to_delete)
    
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"✅ Deleted {deleted_count} employees")
    show_current_data()
    
    spark.stop()


def example_2_delete_by_spark_departments():
    """Example 2: Delete by departments from Spark DataFrame."""
    
    print("\n" + "="*70)
    print("EXAMPLE 2: DELETE BY DEPARTMENTS FROM SPARK DATAFRAME")  
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    spark = create_spark_session()
    
    # Create Spark DataFrame with departments to close
    dept_data = [('Marketing',), ('HR',)]
    dept_schema = StructType([StructField("department", StringType(), True)])
    dept_df = spark.createDataFrame(dept_data, dept_schema)
    
    print("Spark DataFrame with departments to close:")
    dept_df.show()
    
    # Get departments list
    departments = [row.department for row in dept_df.collect()]
    print(f"Departments to close: {departments}")
    
    # Delete from SQL
    conn = sqlite3.connect('spark_sample.db')
    placeholders = ','.join(['?' for _ in departments])
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM employees WHERE department IN ({placeholders})", departments)
    
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"✅ Deleted {deleted_count} employees from closed departments")
    show_current_data()
    
    spark.stop()


def example_3_delete_high_earners():
    """Example 3: Delete high earners using Spark DataFrame filtering."""
    
    print("\n" + "="*70)
    print("EXAMPLE 3: DELETE HIGH EARNERS USING SPARK ANALYSIS")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    spark = create_spark_session()
    
    # Load SQL data into Spark DataFrame for analysis
    conn = sqlite3.connect('spark_sample.db')
    pandas_df = pd.read_sql_query("SELECT * FROM employees", conn)
    conn.close()
    
    sql_spark_df = spark.createDataFrame(pandas_df)
    
    print("Current employee data in Spark:")
    sql_spark_df.show()
    
    # Use Spark to identify high earners (top 25%)
    total_count = sql_spark_df.count()
    salary_75th_percentile = sql_spark_df.approxQuantile("salary", [0.75], 0.01)[0]
    
    print(f"75th percentile salary: ${salary_75th_percentile:,.2f}")
    
    # Filter high earners
    high_earners_df = sql_spark_df.filter(col("salary") >= salary_75th_percentile)
    
    print("High earners to remove:")
    high_earners_df.select("id", "name", "department", "salary").show()
    
    # Get IDs of high earners
    high_earner_ids = [row.id for row in high_earners_df.select("id").collect()]
    
    if high_earner_ids:
        # Delete from SQL database
        conn = sqlite3.connect('spark_sample.db')
        placeholders = ','.join(['?' for _ in high_earner_ids])
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", high_earner_ids)
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"✅ Deleted {deleted_count} high earners")
    
    show_current_data()
    spark.stop()


def example_4_spark_sql_analysis_delete():
    """Example 4: Use Spark SQL for complex deletion criteria."""
    
    print("\n" + "="*70)
    print("EXAMPLE 4: SPARK SQL ANALYSIS FOR DELETION")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    spark = create_spark_session()
    
    # Load data into Spark
    conn = sqlite3.connect('spark_sample.db')
    pandas_df = pd.read_sql_query("SELECT * FROM employees", conn)
    conn.close()
    
    employees_df = spark.createDataFrame(pandas_df)
    
    # Create temporary view for SQL queries
    employees_df.createOrReplaceTempView("employees_spark")
    
    # Use Spark SQL to find employees to delete
    # Example: Delete employees in departments with average salary > $80k
    dept_avg_salary = spark.sql("""
        SELECT department, AVG(salary) as avg_salary, COUNT(*) as emp_count
        FROM employees_spark 
        GROUP BY department
        HAVING AVG(salary) > 80000
    """)
    
    print("Departments with high average salary:")
    dept_avg_salary.show()
    
    # Get employees in these high-salary departments
    high_salary_depts = [row.department for row in dept_avg_salary.select("department").collect()]
    
    if high_salary_depts:
        employees_to_delete = spark.sql(f"""
            SELECT id, name, department, salary
            FROM employees_spark 
            WHERE department IN ({','.join([f"'{dept}'" for dept in high_salary_depts])})
        """)
        
        print("Employees to delete from high-salary departments:")
        employees_to_delete.show()
        
        # Get IDs to delete
        ids_to_delete = [row.id for row in employees_to_delete.select("id").collect()]
        
        # Delete from SQL
        conn = sqlite3.connect('spark_sample.db')
        placeholders = ','.join(['?' for _ in ids_to_delete])
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", ids_to_delete)
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"✅ Deleted {deleted_count} employees from high-salary departments")
    
    show_current_data()
    spark.stop()


def example_5_batch_processing():
    """Example 5: Batch processing for large datasets."""
    
    print("\n" + "="*70)
    print("EXAMPLE 5: BATCH PROCESSING WITH SPARK")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    spark = create_spark_session()
    
    # Create a larger Spark DataFrame for batch demonstration
    large_ids = [(i,) for i in range(1, 13)]  # All employee IDs
    large_schema = StructType([StructField("id", IntegerType(), True)])
    large_df = spark.createDataFrame(large_ids, large_schema)
    
    print("Large Spark DataFrame with IDs to process:")
    large_df.show()
    
    # Process in batches
    batch_size = 3
    all_ids = [row.id for row in large_df.collect()]
    
    print(f"Processing {len(all_ids)} IDs in batches of {batch_size}")
    
    total_deleted = 0
    conn = sqlite3.connect('spark_sample.db')
    cursor = conn.cursor()
    
    for i in range(0, len(all_ids), batch_size):
        batch_ids = all_ids[i:i + batch_size]
        
        print(f"\nProcessing batch {i//batch_size + 1}: IDs {batch_ids}")
        
        # Delete batch
        placeholders = ','.join(['?' for _ in batch_ids])
        cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", batch_ids)
        
        batch_deleted = cursor.rowcount
        total_deleted += batch_deleted
        
        print(f"Deleted {batch_deleted} records in this batch")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Total deleted across all batches: {total_deleted}")
    show_current_data()
    
    spark.stop()


def example_6_spark_dataframe_join_delete():
    """Example 6: Use Spark DataFrame joins to identify deletion candidates."""
    
    print("\n" + "="*70)
    print("EXAMPLE 6: SPARK DATAFRAME JOIN FOR DELETION")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    spark = create_spark_session()
    
    # Load current employees into Spark
    conn = sqlite3.connect('spark_sample.db')
    pandas_df = pd.read_sql_query("SELECT * FROM employees", conn)
    conn.close()
    
    current_employees_df = spark.createDataFrame(pandas_df)
    
    # Create a "termination list" Spark DataFrame
    termination_data = [
        ('alice@company.com', 'Voluntary resignation'),
        ('frank@company.com', 'Performance issues'),
        ('liam@company.com', 'Position eliminated')
    ]
    
    termination_schema = StructType([
        StructField("email", StringType(), True),
        StructField("reason", StringType(), True)
    ])
    
    termination_df = spark.createDataFrame(termination_data, termination_schema)
    
    print("Termination list:")
    termination_df.show()
    
    print("Current employees:")
    current_employees_df.select("id", "name", "email", "department").show()
    
    # Join to find employees to terminate
    employees_to_terminate = current_employees_df.join(
        termination_df, 
        current_employees_df.email == termination_df.email, 
        "inner"
    ).select(
        current_employees_df.id,
        current_employees_df.name, 
        current_employees_df.email,
        termination_df.reason
    )
    
    print("Employees to terminate (from join):")
    employees_to_terminate.show()
    
    # Get IDs to delete
    ids_to_delete = [row.id for row in employees_to_terminate.select("id").collect()]
    
    if ids_to_delete:
        # Delete from SQL
        conn = sqlite3.connect('spark_sample.db')
        placeholders = ','.join(['?' for _ in ids_to_delete])
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", ids_to_delete)
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"✅ Deleted {deleted_count} employees based on termination list")
    
    show_current_data()
    spark.stop()


def show_spark_patterns():
    """Show reusable Spark patterns for SQL deletion."""
    
    print("\n" + "="*70)
    print("📋 REUSABLE SPARK PATTERNS")
    print("="*70)
    
    patterns = '''
# Pattern 1: Simple ID deletion from Spark DataFrame
def delete_by_spark_ids(spark_df, id_column, sql_table, sql_connection):
    """Delete SQL rows by IDs from Spark DataFrame."""
    ids = [row[id_column] for row in spark_df.select(id_column).collect()]
    
    placeholders = ','.join(['?' for _ in ids])
    query = f"DELETE FROM {sql_table} WHERE id IN ({placeholders})"
    
    cursor = sql_connection.cursor()
    cursor.execute(query, ids)
    sql_connection.commit()
    return cursor.rowcount

# Pattern 2: Delete by filtered Spark DataFrame
def delete_by_spark_filter(spark_df, filter_condition, id_column, sql_table, sql_connection):
    """Delete SQL rows based on Spark DataFrame filtering."""
    filtered_df = spark_df.filter(filter_condition)
    ids = [row[id_column] for row in filtered_df.select(id_column).collect()]
    
    if not ids:
        return 0
    
    placeholders = ','.join(['?' for _ in ids])
    query = f"DELETE FROM {sql_table} WHERE id IN ({placeholders})"
    
    cursor = sql_connection.cursor()
    cursor.execute(query, ids)
    sql_connection.commit()
    return cursor.rowcount

# Pattern 3: Batch processing from Spark
def batch_delete_from_spark(spark_df, id_column, sql_table, sql_connection, batch_size=1000):
    """Process large Spark DataFrame deletions in batches."""
    all_ids = [row[id_column] for row in spark_df.select(id_column).collect()]
    
    total_deleted = 0
    cursor = sql_connection.cursor()
    
    for i in range(0, len(all_ids), batch_size):
        batch = all_ids[i:i + batch_size]
        placeholders = ','.join(['?' for _ in batch])
        query = f"DELETE FROM {sql_table} WHERE id IN ({placeholders})"
        
        cursor.execute(query, batch)
        total_deleted += cursor.rowcount
    
    sql_connection.commit()
    return total_deleted

# Pattern 4: Spark SQL analysis for deletion
def analyze_and_delete_with_spark_sql(spark, sql_table, sql_connection, analysis_query):
    """Use Spark SQL to analyze and identify records for deletion."""
    # Load SQL table into Spark
    pandas_df = pd.read_sql_query(f"SELECT * FROM {sql_table}", sql_connection)
    spark_df = spark.createDataFrame(pandas_df)
    spark_df.createOrReplaceTempView("target_table")
    
    # Run analysis query to get IDs to delete
    result_df = spark.sql(analysis_query)
    ids_to_delete = [row.id for row in result_df.select("id").collect()]
    
    if not ids_to_delete:
        return 0
    
    # Delete from SQL
    placeholders = ','.join(['?' for _ in ids_to_delete])
    delete_query = f"DELETE FROM {sql_table} WHERE id IN ({placeholders})"
    
    cursor = sql_connection.cursor()
    cursor.execute(delete_query, ids_to_delete)
    sql_connection.commit()
    return cursor.rowcount
    '''
    
    print(patterns)


def cleanup():
    """Remove sample database file."""
    if os.path.exists('spark_sample.db'):
        os.remove('spark_sample.db')
        print("🧹 Cleaned up sample database")


def main():
    """Run all Spark SQL deletion examples."""
    
    print("SPARK DATAFRAME TO SQL DELETION - SIMPLE EXAMPLES")
    print("="*70)
    
    try:
        # Setup
        setup_sample_database()
        show_current_data()
        
        # Run examples
        example_1_delete_by_spark_ids()
        example_2_delete_by_spark_departments()
        example_3_delete_high_earners()
        example_4_spark_sql_analysis_delete()
        example_5_batch_processing()
        example_6_spark_dataframe_join_delete()
        
        # Show patterns
        show_spark_patterns()
        
        print("\n" + "="*70)
        print("✅ ALL SPARK EXAMPLES COMPLETED!")
        print("="*70)
        
        print("\nKey takeaways:")
        print("• Use Spark for large-scale distributed data processing")
        print("• Be careful with collect() - only use on small datasets")
        print("• Leverage Spark SQL for complex analysis before deletion") 
        print("• Process large deletions in batches")
        print("• Use DataFrame joins to combine multiple data sources")
        print("• Always use parameterized queries for SQL operations")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        cleanup()


if __name__ == "__main__":
    main()