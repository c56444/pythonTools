#!/usr/bin/env python3
"""
SQL Table Row Deletion using Spark DataFrame Values

This script deletes rows from SQL tables based on values from Apache Spark DataFrames.
Supports multiple database types with efficient distributed processing.

Required packages:
    pip install pyspark pandas sqlalchemy pymssql psycopg2-binary mysql-connector-python

Supported databases:
- SQLite
- SQL Server  
- PostgreSQL
- MySQL

Usage:
    python delete_sql_rows_from_spark.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, lit
from pyspark.sql.types import *
import pandas as pd
from sqlalchemy import create_engine, text
import sqlite3
from typing import List, Dict, Any, Optional, Union
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkSQLDeleter:
    """Delete rows from SQL tables using Spark DataFrame values."""
    
    def __init__(self, app_name: str = "SQLRowDeleter"):
        """Initialize Spark session and SQL connections."""
        self.spark = None
        self.sql_connection = None
        self.sql_engine = None
        self.db_type = None
        self.connection_string = None
        self.app_name = app_name
        
    def initialize_spark(self, 
                        config: Dict[str, str] = None,
                        enable_sql_extensions: bool = True) -> bool:
        """Initialize Spark session with optional configurations."""
        try:
            builder = SparkSession.builder.appName(self.app_name)
            
            # Add default configurations
            builder = builder.config("spark.sql.adaptive.enabled", "true")
            builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
            # Add SQL database drivers
            if enable_sql_extensions:
                # Add JDBC drivers for various databases
                builder = builder.config("spark.jars.packages", 
                    "org.postgresql:postgresql:42.5.1,"
                    "com.microsoft.sqlserver:mssql-jdbc:11.2.1.jre8,"
                    "mysql:mysql-connector-java:8.0.33"
                )
            
            # Add custom configurations
            if config:
                for key, value in config.items():
                    builder = builder.config(key, value)
            
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")  # Reduce log noise
            
            logger.info("✅ Spark session initialized successfully")
            logger.info(f"   Spark version: {self.spark.version}")
            logger.info(f"   App name: {self.app_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark session: {e}")
            return False
    
    def connect_to_sql(self, connection_string: str, db_type: str = 'sqlite') -> bool:
        """Connect to SQL database for direct operations."""
        self.connection_string = connection_string
        self.db_type = db_type.lower()
        
        try:
            if self.db_type == 'sqlite':
                self.sql_connection = sqlite3.connect(connection_string)
                self.sql_engine = create_engine(f'sqlite:///{connection_string}')
            else:
                self.sql_engine = create_engine(connection_string)
            
            logger.info(f"✅ Connected to {self.db_type.upper()} database")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to SQL database: {e}")
            return False
    
    def create_sample_data(self) -> None:
        """Create sample data in both Spark and SQL for demonstration."""
        
        # Create sample SQL table
        if self.db_type == 'sqlite':
            cursor = self.sql_connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    department TEXT,
                    salary REAL,
                    hire_date TEXT,
                    active BOOLEAN DEFAULT 1
                )
            ''')
            
            # Insert sample data
            sample_employees = [
                (1, 'John Doe', 'john.doe@company.com', 'Engineering', 85000, '2023-01-15', 1),
                (2, 'Jane Smith', 'jane.smith@company.com', 'Marketing', 72000, '2023-02-01', 1),
                (3, 'Mike Johnson', 'mike.johnson@company.com', 'Finance', 78000, '2023-01-20', 1),
                (4, 'Sarah Wilson', 'sarah.wilson@company.com', 'Engineering', 92000, '2023-03-01', 1),
                (5, 'Tom Brown', 'tom.brown@company.com', 'HR', 68000, '2023-02-15', 1),
                (6, 'Lisa Davis', 'lisa.davis@company.com', 'Marketing', 75000, '2023-01-10', 0),
                (7, 'Chris Miller', 'chris.miller@company.com', 'Finance', 81000, '2023-03-10', 1),
                (8, 'Amy Garcia', 'amy.garcia@company.com', 'Engineering', 88000, '2023-02-20', 1),
                (9, 'David Lee', 'david.lee@company.com', 'HR', 70000, '2023-01-25', 1),
                (10, 'Emma Taylor', 'emma.taylor@company.com', 'Finance', 79000, '2023-03-05', 1),
                (11, 'Alex Johnson', 'alex.johnson@company.com', 'Engineering', 95000, '2023-04-01', 1),
                (12, 'Maria Garcia', 'maria.garcia@company.com', 'Marketing', 73000, '2023-04-15', 1),
                (13, 'James Wilson', 'james.wilson@company.com', 'Finance', 82000, '2023-05-01', 1),
                (14, 'Linda Chen', 'linda.chen@company.com', 'HR', 71000, '2023-05-15', 1),
                (15, 'Robert Kim', 'robert.kim@company.com', 'Engineering', 90000, '2023-06-01', 1)
            ]
            
            cursor.executemany('''
                INSERT OR REPLACE INTO employees 
                (id, name, email, department, salary, hire_date, active)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', sample_employees)
            
            self.sql_connection.commit()
            
        logger.info("✅ Sample SQL data created")
    
    def create_sample_spark_dataframes(self) -> Dict[str, Any]:
        """Create sample Spark DataFrames for deletion examples."""
        
        # DataFrame 1: Employee IDs to delete
        ids_to_delete_data = [
            (2, 'Resigned', '2024-12-01'),
            (4, 'Performance Issues', '2024-12-05'), 
            (6, 'Already Inactive', '2024-12-10'),
            (8, 'Transferred', '2024-12-15')
        ]
        
        ids_to_delete_schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("reason", StringType(), True),
            StructField("termination_date", StringType(), True)
        ])
        
        ids_df = self.spark.createDataFrame(ids_to_delete_data, ids_to_delete_schema)
        
        # DataFrame 2: Departments to close
        dept_closure_data = [
            ('Marketing', '2024-12-31', 'Budget cuts'),
            ('HR', '2024-12-31', 'Outsourcing')
        ]
        
        dept_closure_schema = StructType([
            StructField("department", StringType(), True),
            StructField("closure_date", StringType(), True),
            StructField("reason", StringType(), True)
        ])
        
        dept_df = self.spark.createDataFrame(dept_closure_data, dept_closure_schema)
        
        # DataFrame 3: Specific employees (multiple criteria)
        specific_employees_data = [
            ('John Doe', 'Engineering', 85000),
            ('Lisa Davis', 'Marketing', 75000)
        ]
        
        specific_schema = StructType([
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", DoubleType(), True)
        ])
        
        specific_df = self.spark.createDataFrame(specific_employees_data, specific_schema)
        
        # DataFrame 4: Large dataset for batch processing
        large_ids = [(i,) for i in range(10, 16)]  # IDs 10-15
        large_schema = StructType([StructField("id", IntegerType(), True)])
        large_df = self.spark.createDataFrame(large_ids, large_schema)
        
        logger.info("✅ Sample Spark DataFrames created")
        
        return {
            'ids_to_delete': ids_df,
            'dept_closure': dept_df,
            'specific_employees': specific_df,
            'large_dataset': large_df
        }
    
    def get_sql_table_as_spark_df(self, table_name: str) -> Any:
        """Read SQL table into Spark DataFrame."""
        try:
            if self.db_type == 'sqlite':
                # For SQLite, read via pandas then convert to Spark
                pandas_df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.sql_connection)
                spark_df = self.spark.createDataFrame(pandas_df)
            else:
                # For other databases, use JDBC
                spark_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.connection_string) \
                    .option("dbtable", table_name) \
                    .load()
            
            logger.info(f"📊 Loaded table '{table_name}' as Spark DataFrame: {spark_df.count()} rows")
            return spark_df
            
        except Exception as e:
            logger.error(f"❌ Failed to load table as Spark DataFrame: {e}")
            return None
    
    def delete_by_ids_from_spark(self, 
                                table_name: str, 
                                spark_df: Any, 
                                id_column: str = 'employee_id',
                                sql_id_column: str = 'id',
                                batch_size: int = 1000) -> int:
        """
        Delete SQL rows where IDs match values in Spark DataFrame.
        
        Args:
            table_name: SQL table name
            spark_df: Spark DataFrame containing IDs to delete
            id_column: Column name in Spark DataFrame
            sql_id_column: Column name in SQL table
            batch_size: Number of IDs per batch
        """
        try:
            # Collect unique IDs from Spark DataFrame
            ids_to_delete = spark_df.select(id_column).distinct().rdd.flatMap(lambda x: x).collect()
            
            if not ids_to_delete:
                logger.warning(f"No IDs found in column '{id_column}'")
                return 0
            
            logger.info(f"🗑️  Deleting {len(ids_to_delete)} rows from table '{table_name}'")
            
            total_deleted = 0
            
            # Process in batches
            for i in range(0, len(ids_to_delete), batch_size):
                batch = ids_to_delete[i:i + batch_size]
                
                if self.db_type == 'sqlite':
                    placeholders = ','.join(['?' for _ in batch])
                    query = f"DELETE FROM {table_name} WHERE {sql_id_column} IN ({placeholders})"
                    
                    cursor = self.sql_connection.cursor()
                    cursor.execute(query, batch)
                    deleted_count = cursor.rowcount
                    self.sql_connection.commit()
                
                else:
                    # SQLAlchemy approach for other databases
                    placeholders = ','.join([f':id_{j}' for j in range(len(batch))])
                    query = f"DELETE FROM {table_name} WHERE {sql_id_column} IN ({placeholders})"
                    params = {f'id_{j}': int(id_val) for j, id_val in enumerate(batch)}
                    
                    with self.sql_engine.connect() as conn:
                        result = conn.execute(text(query), params)
                        deleted_count = result.rowcount
                        conn.commit()
                
                total_deleted += deleted_count
                logger.info(f"   Batch {i//batch_size + 1}: Deleted {deleted_count} rows")
            
            logger.info(f"✅ Total rows deleted: {total_deleted}")
            return total_deleted
            
        except Exception as e:
            logger.error(f"❌ Failed to delete by IDs: {e}")
            return 0
    
    def delete_by_department_from_spark(self, 
                                       table_name: str, 
                                       spark_df: Any, 
                                       dept_column: str = 'department') -> int:
        """Delete all rows in departments specified in Spark DataFrame."""
        try:
            # Get unique departments from Spark DataFrame
            departments = spark_df.select(dept_column).distinct().rdd.flatMap(lambda x: x).collect()
            
            if not departments:
                logger.warning(f"No departments found in column '{dept_column}'")
                return 0
            
            logger.info(f"🗑️  Deleting all employees from departments: {departments}")
            
            if self.db_type == 'sqlite':
                placeholders = ','.join(['?' for _ in departments])
                query = f"DELETE FROM {table_name} WHERE department IN ({placeholders})"
                
                cursor = self.sql_connection.cursor()
                cursor.execute(query, departments)
                deleted_count = cursor.rowcount
                self.sql_connection.commit()
            
            else:
                placeholders = ','.join([f':dept_{j}' for j in range(len(departments))])
                query = f"DELETE FROM {table_name} WHERE department IN ({placeholders})"
                params = {f'dept_{j}': dept for j, dept in enumerate(departments)}
                
                with self.sql_engine.connect() as conn:
                    result = conn.execute(text(query), params)
                    deleted_count = result.rowcount
                    conn.commit()
            
            logger.info(f"✅ Deleted {deleted_count} employees from closed departments")
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Failed to delete by department: {e}")
            return 0
    
    def delete_by_multiple_criteria_from_spark(self, 
                                             table_name: str, 
                                             spark_df: Any, 
                                             criteria_mapping: Dict[str, str]) -> int:
        """
        Delete rows matching multiple criteria from Spark DataFrame.
        
        Args:
            table_name: SQL table name
            spark_df: Spark DataFrame with criteria values
            criteria_mapping: Dict mapping spark_column -> sql_column
        """
        try:
            # Convert Spark DataFrame to list of dictionaries
            rows_to_delete = spark_df.select(*criteria_mapping.keys()).collect()
            
            if not rows_to_delete:
                logger.warning("No rows found for deletion criteria")
                return 0
            
            logger.info(f"🗑️  Deleting {len(rows_to_delete)} specific employees")
            
            total_deleted = 0
            
            if self.db_type == 'sqlite':
                cursor = self.sql_connection.cursor()
                
                for row in rows_to_delete:
                    conditions = []
                    params = []
                    
                    for spark_col, sql_col in criteria_mapping.items():
                        conditions.append(f"{sql_col} = ?")
                        params.append(row[spark_col])
                    
                    where_clause = " AND ".join(conditions)
                    query = f"DELETE FROM {table_name} WHERE {where_clause}"
                    
                    cursor.execute(query, params)
                    total_deleted += cursor.rowcount
                
                self.sql_connection.commit()
            
            else:
                with self.sql_engine.connect() as conn:
                    for i, row in enumerate(rows_to_delete):
                        conditions = []
                        params = {}
                        
                        for spark_col, sql_col in criteria_mapping.items():
                            param_name = f'param_{i}_{spark_col}'
                            conditions.append(f"{sql_col} = :{param_name}")
                            params[param_name] = row[spark_col]
                        
                        where_clause = " AND ".join(conditions)
                        query = f"DELETE FROM {table_name} WHERE {where_clause}"
                        
                        result = conn.execute(text(query), params)
                        total_deleted += result.rowcount
                    
                    conn.commit()
            
            logger.info(f"✅ Deleted {total_deleted} employees using multiple criteria")
            return total_deleted
            
        except Exception as e:
            logger.error(f"❌ Failed to delete by multiple criteria: {e}")
            return 0
    
    def preview_deletion_with_spark(self, 
                                   table_name: str, 
                                   spark_df: Any, 
                                   id_column: str = 'employee_id',
                                   sql_id_column: str = 'id') -> Any:
        """Preview which SQL rows would be deleted based on Spark DataFrame."""
        try:
            # Get IDs from Spark DataFrame
            ids_to_delete = spark_df.select(id_column).distinct().rdd.flatMap(lambda x: x).collect()
            
            if not ids_to_delete:
                logger.warning("No IDs found for preview")
                return None
            
            # Query SQL database for matching rows
            if self.db_type == 'sqlite':
                placeholders = ','.join(['?' for _ in ids_to_delete])
                query = f"SELECT * FROM {table_name} WHERE {sql_id_column} IN ({placeholders})"
                preview_df = pd.read_sql_query(query, self.sql_connection, params=ids_to_delete)
                
                # Convert to Spark DataFrame for consistency
                spark_preview_df = self.spark.createDataFrame(preview_df)
            
            else:
                placeholders = ','.join([f':id_{j}' for j in range(len(ids_to_delete))])
                query = f"SELECT * FROM {table_name} WHERE {sql_id_column} IN ({placeholders})"
                params = {f'id_{j}': int(id_val) for j, id_val in enumerate(ids_to_delete)}
                
                preview_df = pd.read_sql_query(query, self.sql_engine, params=params)
                spark_preview_df = self.spark.createDataFrame(preview_df)
            
            logger.info(f"🔍 Preview: {spark_preview_df.count()} rows would be deleted")
            return spark_preview_df
            
        except Exception as e:
            logger.error(f"❌ Failed to preview deletion: {e}")
            return None
    
    def spark_sql_join_delete(self, 
                             table_name: str, 
                             spark_df: Any, 
                             join_condition: str) -> int:
        """
        Advanced: Use Spark SQL to identify and delete rows via join logic.
        
        Args:
            table_name: SQL table name
            spark_df: Spark DataFrame to join with
            join_condition: SQL join condition
        """
        try:
            # Load SQL table into Spark
            sql_spark_df = self.get_sql_table_as_spark_df(table_name)
            if sql_spark_df is None:
                return 0
            
            # Register temporary views
            sql_spark_df.createOrReplaceTempView("sql_table")
            spark_df.createOrReplaceTempView("delete_criteria")
            
            # Find rows to delete using Spark SQL
            query = f"""
                SELECT sql_table.id 
                FROM sql_table 
                INNER JOIN delete_criteria ON {join_condition}
            """
            
            ids_to_delete_df = self.spark.sql(query)
            ids_list = ids_to_delete_df.rdd.flatMap(lambda x: x).collect()
            
            if not ids_list:
                logger.info("No matching rows found for deletion")
                return 0
            
            logger.info(f"🗑️  Found {len(ids_list)} rows to delete via Spark SQL join")
            
            # Delete from SQL database
            if self.db_type == 'sqlite':
                placeholders = ','.join(['?' for _ in ids_list])
                delete_query = f"DELETE FROM {table_name} WHERE id IN ({placeholders})"
                
                cursor = self.sql_connection.cursor()
                cursor.execute(delete_query, ids_list)
                deleted_count = cursor.rowcount
                self.sql_connection.commit()
            
            else:
                placeholders = ','.join([f':id_{j}' for j in range(len(ids_list))])
                delete_query = f"DELETE FROM {table_name} WHERE id IN ({placeholders})"
                params = {f'id_{j}': int(id_val) for j, id_val in enumerate(ids_list)}
                
                with self.sql_engine.connect() as conn:
                    result = conn.execute(text(delete_query), params)
                    deleted_count = result.rowcount
                    conn.commit()
            
            logger.info(f"✅ Deleted {deleted_count} rows using Spark SQL join")
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Failed Spark SQL join delete: {e}")
            return 0
    
    def show_current_sql_data(self, table_name: str = 'employees') -> None:
        """Display current SQL table data."""
        try:
            if self.db_type == 'sqlite':
                df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id", self.sql_connection)
            else:
                df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id", self.sql_engine)
            
            print(f"\n📊 Current {table_name} data:")
            print(df.to_string(index=False))
            print(f"Total records: {len(df)}")
            
        except Exception as e:
            logger.error(f"❌ Failed to show current data: {e}")
    
    def cleanup(self) -> None:
        """Clean up resources."""
        try:
            if self.sql_connection:
                self.sql_connection.close()
            if self.sql_engine:
                self.sql_engine.dispose()
            if self.spark:
                self.spark.stop()
            
            logger.info("🔒 Resources cleaned up successfully")
            
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")


def demonstrate_spark_sql_deletion():
    """Demonstrate various Spark DataFrame to SQL deletion methods."""
    
    print("=" * 90)
    print("🚀 SPARK DATAFRAME TO SQL TABLE DELETION DEMONSTRATION")
    print("=" * 90)
    
    # Initialize the deleter
    deleter = SparkSQLDeleter("SQL_Row_Deletion_Demo")
    
    # Initialize Spark
    if not deleter.initialize_spark():
        print("❌ Failed to initialize Spark")
        return
    
    # Connect to SQLite for demo
    if not deleter.connect_to_sql('spark_demo.db', 'sqlite'):
        print("❌ Failed to connect to database")
        deleter.cleanup()
        return
    
    try:
        # Create sample data
        deleter.create_sample_data()
        deleter.show_current_sql_data()
        
        # Create sample Spark DataFrames
        spark_dfs = deleter.create_sample_spark_dataframes()
        
        # Example 1: Delete by IDs from Spark DataFrame
        print("\n" + "="*80)
        print("1. DELETE BY EMPLOYEE IDs FROM SPARK DATAFRAME")
        print("="*80)
        
        ids_df = spark_dfs['ids_to_delete']
        print("Spark DataFrame with IDs to delete:")
        ids_df.show()
        
        # Preview deletion
        preview_df = deleter.preview_deletion_with_spark('employees', ids_df, 'employee_id', 'id')
        if preview_df:
            print("Rows that will be deleted:")
            preview_df.select('id', 'name', 'department', 'salary').show()
        
        # Perform deletion
        deleted_count = deleter.delete_by_ids_from_spark('employees', ids_df, 'employee_id', 'id')
        deleter.show_current_sql_data()
        
        # Example 2: Delete by department from Spark DataFrame
        print("\n" + "="*80)
        print("2. DELETE BY DEPARTMENT FROM SPARK DATAFRAME")
        print("="*80)
        
        # Reset data
        deleter.create_sample_data()
        
        dept_df = spark_dfs['dept_closure']
        print("Spark DataFrame with departments to close:")
        dept_df.show()
        
        deleted_count = deleter.delete_by_department_from_spark('employees', dept_df, 'department')
        deleter.show_current_sql_data()
        
        # Example 3: Delete by multiple criteria
        print("\n" + "="*80)
        print("3. DELETE BY MULTIPLE CRITERIA FROM SPARK DATAFRAME")
        print("="*80)
        
        # Reset data
        deleter.create_sample_data()
        
        specific_df = spark_dfs['specific_employees']
        print("Spark DataFrame with specific employees:")
        specific_df.show()
        
        criteria_mapping = {
            'name': 'name',
            'department': 'department'
        }
        
        deleted_count = deleter.delete_by_multiple_criteria_from_spark(
            'employees', specific_df, criteria_mapping
        )
        deleter.show_current_sql_data()
        
        # Example 4: Spark SQL join delete
        print("\n" + "="*80)
        print("4. ADVANCED: SPARK SQL JOIN DELETE")
        print("="*80)
        
        # Reset data
        deleter.create_sample_data()
        
        # Create a DataFrame with salary thresholds by department
        salary_criteria_data = [
            ('Engineering', 90000),
            ('Finance', 80000)
        ]
        
        salary_criteria_schema = StructType([
            StructField("department", StringType(), True),
            StructField("salary_threshold", DoubleType(), True)
        ])
        
        salary_df = deleter.spark.createDataFrame(salary_criteria_data, salary_criteria_schema)
        print("Salary criteria DataFrame:")
        salary_df.show()
        
        # Join condition: delete employees earning above threshold in their department
        join_condition = "sql_table.department = delete_criteria.department AND sql_table.salary >= delete_criteria.salary_threshold"
        
        deleted_count = deleter.spark_sql_join_delete('employees', salary_df, join_condition)
        deleter.show_current_sql_data()
        
        # Example 5: Large dataset batch processing
        print("\n" + "="*80)
        print("5. BATCH PROCESSING FOR LARGE DATASETS")
        print("="*80)
        
        # Reset data
        deleter.create_sample_data()
        
        large_df = spark_dfs['large_dataset']
        print("Large Spark DataFrame for batch deletion:")
        large_df.show()
        
        deleted_count = deleter.delete_by_ids_from_spark(
            'employees', large_df, 'id', 'id', batch_size=3
        )
        deleter.show_current_sql_data()
        
    except Exception as e:
        logger.error(f"❌ Error during demonstration: {e}")
    
    finally:
        # Cleanup
        deleter.cleanup()
        
        # Remove demo database
        if os.path.exists('spark_demo.db'):
            os.remove('spark_demo.db')
            print("\n🧹 Cleaned up demo database file")
    
    print("\n" + "="*90)
    print("✅ SPARK SQL DELETION DEMONSTRATION COMPLETED")
    print("="*90)


def show_connection_examples_for_spark():
    """Show connection examples for different databases with Spark."""
    
    print("\n" + "="*80)
    print("🔗 SPARK DATABASE CONNECTION EXAMPLES")
    print("="*80)
    
    examples = """
# SQLite (via pandas bridge)
deleter = SparkSQLDeleter()
deleter.initialize_spark()
deleter.connect_to_sql('database.db', 'sqlite')

# SQL Server with JDBC
spark_config = {
    "spark.jars.packages": "com.microsoft.sqlserver:mssql-jdbc:11.2.1.jre8"
}
deleter.initialize_spark(spark_config)

# Read SQL Server table directly into Spark
df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:sqlserver://server:1433;databaseName=mydb") \\
    .option("dbtable", "employees") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .load()

# PostgreSQL with JDBC  
spark_config = {
    "spark.jars.packages": "org.postgresql:postgresql:42.5.1"
}
deleter.initialize_spark(spark_config)

df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:postgresql://localhost:5432/database") \\
    .option("dbtable", "employees") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .load()

# MySQL with JDBC
spark_config = {
    "spark.jars.packages": "mysql:mysql-connector-java:8.0.33"
}
deleter.initialize_spark(spark_config)

df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:mysql://localhost:3306/database") \\
    .option("dbtable", "employees") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .load()
    """
    
    print(examples)


def show_spark_sql_best_practices():
    """Show best practices for Spark SQL operations."""
    
    print("\n" + "="*80)
    print("💡 SPARK SQL DELETION BEST PRACTICES")
    print("="*80)
    
    practices = """
1. OPTIMIZE SPARK CONFIGURATION
   - Enable adaptive query execution
   - Set appropriate number of partitions
   - Configure memory settings for your cluster

2. EFFICIENT DATA COLLECTION
   - Use collect() sparingly - only for small datasets
   - Consider using toPandas() for medium datasets
   - Use write operations for large datasets

3. BATCH PROCESSING
   - Process large deletions in batches
   - Monitor Spark UI for performance bottlenecks
   - Use partitioning to parallelize work

4. MEMORY MANAGEMENT
   - Cache frequently accessed DataFrames: df.cache()
   - Unpersist DataFrames when no longer needed: df.unpersist()
   - Monitor driver and executor memory usage

5. SQL OPTIMIZATION
   - Use broadcast joins for small lookup tables
   - Partition large tables appropriately
   - Use columnar storage formats (Parquet, Delta)

6. ERROR HANDLING
   - Implement retry logic for transient failures
   - Use checkpointing for long-running operations
   - Monitor Spark application logs

7. SECURITY CONSIDERATIONS
   - Use parameterized queries to prevent injection
   - Encrypt database connections
   - Implement proper authentication

8. TESTING AND VALIDATION
   - Test with small datasets first
   - Validate deletion counts
   - Use dry-run modes when available

9. MONITORING
   - Track Spark application metrics
   - Monitor database connection pools
   - Set up alerts for failures

10. RESOURCE CLEANUP
    - Always stop Spark sessions
    - Close database connections
    - Clean up temporary files and tables
    """
    
    print(practices)


def main():
    """Main function to run demonstrations."""
    
    try:
        # Run main demonstration
        demonstrate_spark_sql_deletion()
        
        # Show connection examples
        show_connection_examples_for_spark()
        
        # Show best practices
        show_spark_sql_best_practices()
        
        print("\n🎉 All Spark SQL deletion examples completed!")
        print("\nKey takeaways:")
        print("• Use Spark for large-scale distributed data processing")
        print("• Collect data carefully - avoid bringing large datasets to driver")
        print("• Leverage Spark SQL for complex join-based deletions")
        print("• Always use parameterized queries for security")
        print("• Process large deletions in batches")
        print("• Monitor Spark UI for performance optimization")
        
    except Exception as e:
        logger.error(f"❌ Error in main demonstration: {e}")


if __name__ == "__main__":
    main()