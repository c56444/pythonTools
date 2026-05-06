#!/usr/bin/env python3
"""
Delta Table Row Deletion using Spark DataFrame WHERE Clauses

This script demonstrates how to delete rows from Delta tables using Spark DataFrame
columns to build WHERE clauses. Includes ACID transactions, time travel, and
optimized Delta Lake operations.

Required packages:
    pip install pyspark delta-spark pandas

Delta Lake Features:
- ACID transactions
- Time travel (data versioning)
- Schema evolution
- Optimized file management
- Merge operations

Usage:
    python delete_delta_rows_with_dataframe.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, collect_list, array_contains
from pyspark.sql.types import *
from delta import *
import pandas as pd
import logging
from datetime import datetime, timedelta
import os
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DeltaTableDeleter:
    """Delete rows from Delta tables using Spark DataFrame WHERE clauses."""
    
    def __init__(self, app_name: str = "DeltaTableDeleter"):
        """Initialize Spark session with Delta Lake support."""
        self.spark = None
        self.app_name = app_name
        self.delta_tables_path = "delta_tables"
        
    def initialize_spark_with_delta(self) -> bool:
        """Initialize Spark session with Delta Lake configurations."""
        try:
            builder = SparkSession.builder.appName(self.app_name)
            
            # Delta Lake configurations
            builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            # Performance optimizations
            builder = builder.config("spark.sql.adaptive.enabled", "true")
            builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            builder = builder.config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            
            self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("✅ Spark session initialized with Delta Lake support")
            logger.info(f"   Spark version: {self.spark.version}")
            logger.info(f"   Delta Lake enabled: True")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark with Delta: {e}")
            return False
    
    def create_sample_delta_tables(self) -> bool:
        """Create sample Delta tables for demonstration."""
        try:
            # Create delta_tables directory
            if os.path.exists(self.delta_tables_path):
                shutil.rmtree(self.delta_tables_path)
            os.makedirs(self.delta_tables_path, exist_ok=True)
            
            # Sample employee data
            employee_data = [
                (1, 'John Doe', 'john.doe@company.com', 'Engineering', 95000, '2023-01-15', True, 'Senior'),
                (2, 'Jane Smith', 'jane.smith@company.com', 'Marketing', 72000, '2023-02-01', True, 'Mid'),
                (3, 'Mike Johnson', 'mike.johnson@company.com', 'Finance', 78000, '2023-01-20', True, 'Mid'),
                (4, 'Sarah Wilson', 'sarah.wilson@company.com', 'Engineering', 102000, '2023-03-01', True, 'Senior'),
                (5, 'Tom Brown', 'tom.brown@company.com', 'HR', 68000, '2023-02-15', True, 'Junior'),
                (6, 'Lisa Davis', 'lisa.davis@company.com', 'Marketing', 75000, '2023-01-10', False, 'Mid'),
                (7, 'Chris Miller', 'chris.miller@company.com', 'Finance', 81000, '2023-03-10', True, 'Senior'),
                (8, 'Amy Garcia', 'amy.garcia@company.com', 'Engineering', 88000, '2023-02-20', True, 'Mid'),
                (9, 'David Lee', 'david.lee@company.com', 'HR', 70000, '2023-01-25', True, 'Mid'),
                (10, 'Emma Taylor', 'emma.taylor@company.com', 'Finance', 79000, '2023-03-05', True, 'Mid'),
                (11, 'Alex Chen', 'alex.chen@company.com', 'Engineering', 115000, '2023-04-01', True, 'Senior'),
                (12, 'Maria Rodriguez', 'maria.rodriguez@company.com', 'Marketing', 73000, '2023-04-15', True, 'Mid'),
                (13, 'James Wilson', 'james.wilson@company.com', 'Finance', 82000, '2023-05-01', True, 'Senior'),
                (14, 'Linda Kim', 'linda.kim@company.com', 'HR', 71000, '2023-05-15', True, 'Mid'),
                (15, 'Robert Singh', 'robert.singh@company.com', 'Engineering', 98000, '2023-06-01', True, 'Senior')
            ]
            
            employee_schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
                StructField("hire_date", StringType(), True),
                StructField("active", BooleanType(), True),
                StructField("level", StringType(), True)
            ])
            
            employees_df = self.spark.createDataFrame(employee_data, employee_schema)
            
            # Write as Delta table
            employees_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(f"{self.delta_tables_path}/employees")
            
            # Create projects Delta table
            project_data = [
                (101, 'Project Alpha', 'Engineering', '2023-01-01', '2023-12-31', True),
                (102, 'Marketing Campaign Q1', 'Marketing', '2023-01-01', '2023-03-31', False),
                (103, 'Financial Audit', 'Finance', '2023-02-01', '2023-06-30', True),
                (104, 'HR System Upgrade', 'HR', '2023-03-01', '2023-09-30', True),
                (105, 'Project Beta', 'Engineering', '2023-04-01', '2024-03-31', True),
                (106, 'Marketing Campaign Q2', 'Marketing', '2023-04-01', '2023-06-30', False)
            ]
            
            project_schema = StructType([
                StructField("project_id", IntegerType(), True),
                StructField("project_name", StringType(), True),
                StructField("department", StringType(), True),
                StructField("start_date", StringType(), True),
                StructField("end_date", StringType(), True),
                StructField("active", BooleanType(), True)
            ])
            
            projects_df = self.spark.createDataFrame(project_data, project_schema)
            projects_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(f"{self.delta_tables_path}/projects")
            
            logger.info("✅ Sample Delta tables created")
            logger.info(f"   Employees table: {employees_df.count()} records")
            logger.info(f"   Projects table: {projects_df.count()} records")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create sample Delta tables: {e}")
            return False
    
    def load_delta_table(self, table_name: str):
        """Load a Delta table as DataFrame."""
        try:
            delta_df = self.spark.read.format("delta").load(f"{self.delta_tables_path}/{table_name}")
            logger.info(f"📊 Loaded Delta table '{table_name}': {delta_df.count()} records")
            return delta_df
        except Exception as e:
            logger.error(f"❌ Failed to load Delta table '{table_name}': {e}")
            return None
    
    def show_delta_table_info(self, table_name: str):
        """Show current Delta table contents and metadata."""
        try:
            delta_df = self.load_delta_table(table_name)
            if delta_df:
                print(f"\n📊 Current {table_name} Delta table:")
                delta_df.show()
                print(f"Total records: {delta_df.count()}")
                
                # Show Delta table history
                delta_table = DeltaTable.forPath(self.spark, f"{self.delta_tables_path}/{table_name}")
                print(f"\n📈 Delta table history:")
                delta_table.history().select("version", "timestamp", "operation", "operationParameters").show()
                
        except Exception as e:
            logger.error(f"❌ Failed to show table info: {e}")
    
    def delete_by_dataframe_ids(self, 
                               table_name: str, 
                               ids_df, 
                               df_id_column: str = "employee_id",
                               table_id_column: str = "id") -> bool:
        """
        Delete rows from Delta table where IDs match DataFrame values.
        
        Args:
            table_name: Delta table name
            ids_df: Spark DataFrame containing IDs to delete
            df_id_column: Column name in the DataFrame
            table_id_column: Column name in the Delta table
        """
        try:
            logger.info(f"🗑️  Deleting rows from {table_name} using DataFrame IDs")
            
            # Get the Delta table
            delta_table = DeltaTable.forPath(self.spark, f"{self.delta_tables_path}/{table_name}")
            
            # Show what we're about to delete
            print("IDs to delete:")
            ids_df.show()
            
            # Collect IDs for the WHERE clause
            ids_list = [row[df_id_column] for row in ids_df.select(df_id_column).distinct().collect()]
            
            if not ids_list:
                logger.warning("No IDs found for deletion")
                return False
            
            # Build WHERE condition using DataFrame values
            where_condition = col(table_id_column).isin(ids_list)
            
            # Perform the deletion
            initial_count = delta_table.toDF().count()
            
            delta_table.delete(where_condition)
            
            final_count = delta_table.toDF().count()
            deleted_count = initial_count - final_count
            
            logger.info(f"✅ Deleted {deleted_count} rows from {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete by DataFrame IDs: {e}")
            return False
    
    def delete_by_dataframe_join_condition(self, 
                                         table_name: str, 
                                         criteria_df,
                                         join_columns: dict) -> bool:
        """
        Delete rows using DataFrame join conditions.
        
        Args:
            table_name: Delta table name  
            criteria_df: DataFrame with deletion criteria
            join_columns: Dict mapping table_column -> df_column
        """
        try:
            logger.info(f"🗑️  Deleting rows from {table_name} using DataFrame join conditions")
            
            delta_table = DeltaTable.forPath(self.spark, f"{self.delta_tables_path}/{table_name}")
            
            print("Deletion criteria DataFrame:")
            criteria_df.show()
            
            # Build join conditions
            join_conditions = []
            for table_col, df_col in join_columns.items():
                join_conditions.append(col(table_col) == criteria_df[df_col])
            
            # Combine conditions with AND
            combined_condition = join_conditions[0]
            for condition in join_conditions[1:]:
                combined_condition = combined_condition & condition
            
            # Get records that match the criteria (for logging)
            table_df = delta_table.toDF()
            matching_records = table_df.join(criteria_df, 
                                           [table_df[table_col] == criteria_df[df_col] 
                                            for table_col, df_col in join_columns.items()], 
                                           "inner")
            
            print("Records that will be deleted:")
            matching_records.select([col for col in table_df.columns]).distinct().show()
            
            # Perform deletion using DataFrame values in WHERE clause
            initial_count = table_df.count()
            
            # Create WHERE condition based on DataFrame values
            criteria_list = []
            for row in criteria_df.collect():
                row_conditions = []
                for table_col, df_col in join_columns.items():
                    row_conditions.append(col(table_col) == lit(row[df_col]))
                
                # Combine row conditions with AND
                row_condition = row_conditions[0]
                for condition in row_conditions[1:]:
                    row_condition = row_condition & condition
                
                criteria_list.append(row_condition)
            
            # Combine all criteria with OR
            if criteria_list:
                final_condition = criteria_list[0]
                for condition in criteria_list[1:]:
                    final_condition = final_condition | condition
                
                delta_table.delete(final_condition)
            
            final_count = delta_table.toDF().count()
            deleted_count = initial_count - final_count
            
            logger.info(f"✅ Deleted {deleted_count} rows using join conditions")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete by join conditions: {e}")
            return False
    
    def delete_by_dataframe_filter_criteria(self, 
                                          table_name: str, 
                                          filter_df,
                                          filter_column: str,
                                          table_filter_column: str = None) -> bool:
        """
        Delete rows where table column values are found in DataFrame filter column.
        
        Args:
            table_name: Delta table name
            filter_df: DataFrame containing filter values
            filter_column: Column in DataFrame with filter values
            table_filter_column: Column in table to filter (defaults to filter_column)
        """
        try:
            if table_filter_column is None:
                table_filter_column = filter_column
                
            logger.info(f"🗑️  Deleting rows from {table_name} where {table_filter_column} in DataFrame values")
            
            delta_table = DeltaTable.forPath(self.spark, f"{self.delta_tables_path}/{table_name}")
            
            print(f"Filter criteria (column: {filter_column}):")
            filter_df.show()
            
            # Get unique filter values from DataFrame
            filter_values = [row[filter_column] for row in filter_df.select(filter_column).distinct().collect()]
            
            if not filter_values:
                logger.warning(f"No filter values found in column {filter_column}")
                return False
            
            print(f"Filter values: {filter_values}")
            
            # Create WHERE condition
            where_condition = col(table_filter_column).isin(filter_values)
            
            # Show what will be deleted
            table_df = delta_table.toDF()
            to_delete = table_df.filter(where_condition)
            print("Records that will be deleted:")
            to_delete.show()
            
            # Perform deletion
            initial_count = table_df.count()
            delta_table.delete(where_condition)
            
            final_count = delta_table.toDF().count()
            deleted_count = initial_count - final_count
            
            logger.info(f"✅ Deleted {deleted_count} rows using filter criteria")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete by filter criteria: {e}")
            return False
    
    def delete_with_complex_dataframe_conditions(self, 
                                               table_name: str, 
                                               conditions_df) -> bool:
        """
        Delete rows using complex conditions derived from DataFrame analysis.
        
        Args:
            table_name: Delta table name
            conditions_df: DataFrame with complex conditions/thresholds
        """
        try:
            logger.info(f"🗑️  Deleting rows from {table_name} using complex DataFrame conditions")
            
            delta_table = DeltaTable.forPath(self.spark, f"{self.delta_tables_path}/{table_name}")
            table_df = delta_table.toDF()
            
            print("Complex conditions DataFrame:")
            conditions_df.show()
            
            # Example: Delete employees in departments with criteria from DataFrame
            # Assuming conditions_df has columns: department, min_salary, max_employees
            
            deletion_conditions = []
            
            for row in conditions_df.collect():
                department = row['department']
                min_salary = row['min_salary'] if 'min_salary' in row.asDict() else None
                max_employees = row['max_employees'] if 'max_employees' in row.asDict() else None
                
                # Build condition for this department
                dept_condition = col('department') == lit(department)
                
                if min_salary is not None:
                    dept_condition = dept_condition & (col('salary') >= lit(min_salary))
                
                deletion_conditions.append(dept_condition)
            
            if deletion_conditions:
                # Combine all department conditions with OR
                final_condition = deletion_conditions[0]
                for condition in deletion_conditions[1:]:
                    final_condition = final_condition | condition
                
                # Show what will be deleted
                to_delete = table_df.filter(final_condition)
                print("Records that will be deleted:")
                to_delete.show()
                
                # Perform deletion
                initial_count = table_df.count()
                delta_table.delete(final_condition)
                
                final_count = delta_table.toDF().count()
                deleted_count = initial_count - final_count
                
                logger.info(f"✅ Deleted {deleted_count} rows using complex conditions")
                return True
            else:
                logger.warning("No valid conditions found")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to delete with complex conditions: {e}")
            return False
    
    def delete_using_dataframe_subquery(self, 
                                       table_name: str, 
                                       subquery_df,
                                       subquery_logic: str) -> bool:
        """
        Delete rows using DataFrame as subquery logic.
        
        Args:
            table_name: Delta table name
            subquery_df: DataFrame used for subquery-like logic
            subquery_logic: Description of the subquery logic
        """
        try:
            logger.info(f"🗑️  Deleting rows from {table_name} using DataFrame subquery logic")
            logger.info(f"   Logic: {subquery_logic}")
            
            delta_table = DeltaTable.forPath(self.spark, f"{self.delta_tables_path}/{table_name}")
            table_df = delta_table.toDF()
            
            print("Subquery DataFrame:")
            subquery_df.show()
            
            # Example subquery logic: Delete employees whose departments 
            # appear in the subquery_df with specific criteria
            
            # Register DataFrames as temp views for SQL-like operations
            table_df.createOrReplaceTempView("main_table")
            subquery_df.createOrReplaceTempView("subquery_table")
            
            # Use Spark SQL to identify records to delete
            records_to_delete = self.spark.sql(f"""
                SELECT main_table.id
                FROM main_table
                WHERE main_table.department IN (
                    SELECT department 
                    FROM subquery_table
                )
            """)
            
            print("Records identified for deletion (via subquery):")
            records_to_delete.show()
            
            # Get IDs to delete
            ids_to_delete = [row.id for row in records_to_delete.collect()]
            
            if ids_to_delete:
                # Create WHERE condition
                where_condition = col('id').isin(ids_to_delete)
                
                # Perform deletion
                initial_count = table_df.count()
                delta_table.delete(where_condition)
                
                final_count = delta_table.toDF().count()
                deleted_count = initial_count - final_count
                
                logger.info(f"✅ Deleted {deleted_count} rows using subquery logic")
                return True
            else:
                logger.info("No records matched the subquery criteria")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to delete using subquery: {e}")
            return False
    
    def demonstrate_delta_time_travel(self, table_name: str):
        """Demonstrate Delta Lake time travel capabilities."""
        try:
            logger.info(f"🕐 Demonstrating Delta time travel for {table_name}")
            
            delta_table = DeltaTable.forPath(self.spark, f"{self.delta_tables_path}/{table_name}")
            
            print("\n📈 Delta table version history:")
            history_df = delta_table.history()
            history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
            
            # Show current version
            current_version = history_df.select("version").collect()[0]["version"]
            print(f"\nCurrent version: {current_version}")
            
            current_df = delta_table.toDF()
            print(f"Current record count: {current_df.count()}")
            
            # Time travel to previous version if available
            if current_version > 0:
                previous_version = current_version - 1
                print(f"\n🕰️  Time traveling to version {previous_version}:")
                
                previous_df = self.spark.read.format("delta") \
                    .option("versionAsOf", previous_version) \
                    .load(f"{self.delta_tables_path}/{table_name}")
                
                print(f"Version {previous_version} record count: {previous_df.count()}")
                previous_df.show()
                
                # Show the difference
                print(f"\nRecord count difference: {previous_df.count() - current_df.count()}")
            
        except Exception as e:
            logger.error(f"❌ Failed to demonstrate time travel: {e}")
    
    def optimize_delta_table(self, table_name: str):
        """Optimize Delta table (compaction, vacuum)."""
        try:
            logger.info(f"🔧 Optimizing Delta table: {table_name}")
            
            delta_table = DeltaTable.forPath(self.spark, f"{self.delta_tables_path}/{table_name}")
            
            # Optimize (compaction)
            delta_table.optimize().executeCompaction()
            logger.info("✅ Table optimization (compaction) completed")
            
            # Vacuum old files (be careful with retention period)
            delta_table.vacuum(0)  # 0 hours for demo - normally use 168 hours (7 days)
            logger.info("✅ Table vacuum completed")
            
        except Exception as e:
            logger.error(f"❌ Failed to optimize table: {e}")
    
    def cleanup_resources(self):
        """Clean up Spark session and temporary files."""
        try:
            if self.spark:
                self.spark.stop()
            
            # Clean up delta tables directory
            if os.path.exists(self.delta_tables_path):
                shutil.rmtree(self.delta_tables_path)
            
            logger.info("🔒 Resources cleaned up successfully")
            
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")


def demonstrate_delta_dataframe_deletions():
    """Demonstrate various Delta table deletion methods using DataFrame WHERE clauses."""
    
    print("=" * 100)
    print("🚀 DELTA TABLE DELETION USING SPARK DATAFRAME WHERE CLAUSES")
    print("=" * 100)
    
    # Initialize the deleter
    deleter = DeltaTableDeleter("Delta_DataFrame_Deletion_Demo")
    
    # Initialize Spark with Delta
    if not deleter.initialize_spark_with_delta():
        print("❌ Failed to initialize Spark with Delta")
        return
    
    try:
        # Create sample Delta tables
        if not deleter.create_sample_delta_tables():
            print("❌ Failed to create sample tables")
            return
        
        # Show initial state
        deleter.show_delta_table_info("employees")
        
        # Example 1: Delete by IDs from DataFrame
        print("\n" + "="*80)
        print("1. DELETE BY IDs FROM DATAFRAME")
        print("="*80)
        
        # Create DataFrame with employee IDs to delete
        ids_data = [(2,), (4,), (6,)]
        ids_schema = StructType([StructField("employee_id", IntegerType(), True)])
        ids_df = deleter.spark.createDataFrame(ids_data, ids_schema)
        
        success = deleter.delete_by_dataframe_ids("employees", ids_df, "employee_id", "id")
        if success:
            deleter.show_delta_table_info("employees")
        
        # Example 2: Delete by department filter from DataFrame
        print("\n" + "="*80)
        print("2. DELETE BY DEPARTMENT FILTER FROM DATAFRAME")
        print("="*80)
        
        # Reset table
        deleter.create_sample_delta_tables()
        
        # Create DataFrame with departments to delete
        dept_data = [('Marketing',), ('HR',)]
        dept_schema = StructType([StructField("department", StringType(), True)])
        dept_df = deleter.spark.createDataFrame(dept_data, dept_schema)
        
        success = deleter.delete_by_dataframe_filter_criteria("employees", dept_df, "department")
        if success:
            deleter.show_delta_table_info("employees")
        
        # Example 3: Delete by join conditions
        print("\n" + "="*80)
        print("3. DELETE BY JOIN CONDITIONS FROM DATAFRAME")
        print("="*80)
        
        # Reset table
        deleter.create_sample_delta_tables()
        
        # Create DataFrame with specific employee criteria
        criteria_data = [
            ('John Doe', 'Engineering'),
            ('Lisa Davis', 'Marketing')
        ]
        criteria_schema = StructType([
            StructField("name", StringType(), True),
            StructField("dept", StringType(), True)
        ])
        criteria_df = deleter.spark.createDataFrame(criteria_data, criteria_schema)
        
        join_columns = {'name': 'name', 'department': 'dept'}
        success = deleter.delete_by_dataframe_join_condition("employees", criteria_df, join_columns)
        if success:
            deleter.show_delta_table_info("employees")
        
        # Example 4: Delete with complex DataFrame conditions
        print("\n" + "="*80)
        print("4. DELETE WITH COMPLEX DATAFRAME CONDITIONS")
        print("="*80)
        
        # Reset table
        deleter.create_sample_delta_tables()
        
        # Create DataFrame with complex deletion criteria
        complex_conditions_data = [
            ('Engineering', 90000),  # Delete Engineering employees with salary >= 90000
            ('Finance', 80000)       # Delete Finance employees with salary >= 80000
        ]
        complex_schema = StructType([
            StructField("department", StringType(), True),
            StructField("min_salary", IntegerType(), True)
        ])
        complex_df = deleter.spark.createDataFrame(complex_conditions_data, complex_schema)
        
        success = deleter.delete_with_complex_dataframe_conditions("employees", complex_df)
        if success:
            deleter.show_delta_table_info("employees")
        
        # Example 5: Delete using DataFrame as subquery
        print("\n" + "="*80)
        print("5. DELETE USING DATAFRAME SUBQUERY LOGIC")
        print("="*80)
        
        # Reset table
        deleter.create_sample_delta_tables()
        
        # Create DataFrame representing projects that are inactive
        # Delete employees in departments that have inactive projects
        inactive_projects_data = [('Marketing',), ('HR',)]
        inactive_schema = StructType([StructField("department", StringType(), True)])
        inactive_projects_df = deleter.spark.createDataFrame(inactive_projects_data, inactive_schema)
        
        subquery_logic = "Delete employees in departments that have inactive projects"
        success = deleter.delete_using_dataframe_subquery("employees", inactive_projects_df, subquery_logic)
        if success:
            deleter.show_delta_table_info("employees")
        
        # Demonstrate Delta Lake features
        print("\n" + "="*80)
        print("6. DELTA LAKE TIME TRAVEL DEMONSTRATION")
        print("="*80)
        
        deleter.demonstrate_delta_time_travel("employees")
        
        # Optimize table
        print("\n" + "="*80)
        print("7. DELTA TABLE OPTIMIZATION")
        print("="*80)
        
        deleter.optimize_delta_table("employees")
        
    except Exception as e:
        logger.error(f"❌ Error during demonstration: {e}")
    
    finally:
        # Cleanup
        deleter.cleanup_resources()
    
    print("\n" + "="*100)
    print("✅ DELTA TABLE DATAFRAME DELETION DEMONSTRATION COMPLETED")
    print("="*100)


def show_delta_dataframe_patterns():
    """Show reusable patterns for Delta table operations with DataFrames."""
    
    print("\n" + "="*80)
    print("📋 REUSABLE DELTA DATAFRAME DELETION PATTERNS")
    print("="*80)
    
    patterns = '''
# Pattern 1: Simple DataFrame ID deletion
def delete_by_dataframe_ids(delta_table, ids_df, df_id_col, table_id_col):
    """Delete rows where table ID matches DataFrame values."""
    ids_list = [row[df_id_col] for row in ids_df.select(df_id_col).distinct().collect()]
    where_condition = col(table_id_col).isin(ids_list)
    delta_table.delete(where_condition)

# Pattern 2: DataFrame filter-based deletion  
def delete_by_dataframe_filter(delta_table, filter_df, filter_col, table_col):
    """Delete rows where table column values exist in DataFrame."""
    filter_values = [row[filter_col] for row in filter_df.select(filter_col).distinct().collect()]
    where_condition = col(table_col).isin(filter_values)
    delta_table.delete(where_condition)

# Pattern 3: Multi-column DataFrame join deletion
def delete_by_dataframe_join(delta_table, criteria_df, join_mapping):
    """Delete rows using multi-column DataFrame join criteria."""
    deletion_conditions = []
    
    for row in criteria_df.collect():
        row_conditions = []
        for table_col, df_col in join_mapping.items():
            row_conditions.append(col(table_col) == lit(row[df_col]))
        
        # Combine row conditions with AND
        row_condition = row_conditions[0]
        for condition in row_conditions[1:]:
            row_condition = row_condition & condition
        
        deletion_conditions.append(row_condition)
    
    # Combine all rows with OR
    if deletion_conditions:
        final_condition = deletion_conditions[0]
        for condition in deletion_conditions[1:]:
            final_condition = final_condition | condition
        
        delta_table.delete(final_condition)

# Pattern 4: Complex analytical deletion
def delete_by_dataframe_analysis(spark, delta_table, analysis_df, logic):
    """Delete rows using complex DataFrame analysis results."""
    # Register DataFrames as temp views
    delta_table.toDF().createOrReplaceTempView("main_table")
    analysis_df.createOrReplaceTempView("analysis_table")
    
    # Use Spark SQL with DataFrame values
    ids_to_delete_df = spark.sql(logic)
    ids_list = [row.id for row in ids_to_delete_df.collect()]
    
    if ids_list:
        where_condition = col('id').isin(ids_list)
        delta_table.delete(where_condition)

# Pattern 5: Batch DataFrame deletion
def batch_delete_from_dataframe(delta_table, large_df, id_col, batch_size=1000):
    """Process large DataFrame deletions in batches."""
    # Get total count for batching
    total_count = large_df.count()
    
    # Process in batches using DataFrame operations
    for i in range(0, total_count, batch_size):
        batch_df = large_df.limit(batch_size).offset(i)
        ids_list = [row[id_col] for row in batch_df.collect()]
        
        if ids_list:
            where_condition = col('id').isin(ids_list)
            delta_table.delete(where_condition)
    '''
    
    print(patterns)


def show_delta_best_practices():
    """Show Delta Lake best practices for DataFrame operations."""
    
    print("\n" + "="*80)
    print("💡 DELTA LAKE DATAFRAME DELETION BEST PRACTICES")
    print("="*80)
    
    practices = """
1. TRANSACTION SAFETY
   - Delta Lake provides ACID transactions automatically
   - Use try/catch for error handling and rollback scenarios
   - Operations are atomic - either all succeed or all fail

2. PERFORMANCE OPTIMIZATION
   - Use Delta table optimization: delta_table.optimize().executeCompaction()
   - Vacuum old files periodically: delta_table.vacuum(retention_hours)
   - Partition large tables by commonly filtered columns

3. SCHEMA EVOLUTION
   - Delta Lake supports schema evolution automatically
   - Add columns without breaking existing queries
   - Use mergeSchema option when needed

4. TIME TRAVEL AND VERSIONING
   - Every operation creates a new version
   - Use time travel for auditing: .option("versionAsOf", version)
   - Query historical data: .option("timestampAsOf", timestamp)

5. EFFICIENT WHERE CLAUSES
   - Use column statistics for better performance
   - Leverage Z-ordering for multi-column filters
   - Avoid collect() on large DataFrames - use aggregations instead

6. BATCH OPERATIONS
   - Process large deletions in batches to avoid memory issues
   - Use DataFrame.coalesce() to control output files
   - Monitor Spark UI for performance bottlenecks

7. DATA QUALITY
   - Use Delta Lake constraints for data validation
   - Implement check constraints: ALTER TABLE ADD CONSTRAINT
   - Validate DataFrame data before deletion operations

8. MONITORING AND LOGGING
   - Track Delta table metrics and file statistics
   - Log deletion operations for audit trails
   - Use Delta Lake data lineage features

9. CONCURRENT OPERATIONS
   - Delta Lake handles concurrent reads/writes automatically
   - Use optimistic concurrency control
   - Handle potential conflicts gracefully

10. SECURITY CONSIDERATIONS
    - Use parameterized conditions to prevent injection
    - Implement proper access controls
    - Audit sensitive deletion operations
    """
    
    print(practices)


def main():
    """Main function to run all demonstrations."""
    
    try:
        # Run main demonstration
        demonstrate_delta_dataframe_deletions()
        
        # Show patterns and best practices
        show_delta_dataframe_patterns()
        show_delta_best_practices()
        
        print("\n🎉 All Delta Lake DataFrame deletion examples completed!")
        print("\nKey takeaways:")
        print("• Delta Lake provides ACID transactions for safe deletions")
        print("• Use DataFrame values efficiently in WHERE clauses")
        print("• Leverage Delta's time travel for recovery and auditing")
        print("• Optimize tables regularly for better performance")
        print("• Process large deletions in batches")
        print("• Monitor operations through Spark UI and Delta metrics")
        
    except Exception as e:
        logger.error(f"❌ Error in main demonstration: {e}")


if __name__ == "__main__":
    main()