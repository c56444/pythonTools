#!/usr/bin/env python3
"""
Simple Delta Table Row Deletion using Spark DataFrame WHERE Clauses

Quick examples for deleting rows from Delta tables using Spark DataFrame
columns in WHERE clauses.

Required packages:
    pip install pyspark delta-spark

Usage:
    python simple_delta_deletion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
from delta import *
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleDeltaDeleter:
    """Simple Delta table deletion using DataFrame WHERE clauses."""
    
    def __init__(self):
        """Initialize with Delta-enabled Spark session."""
        builder = SparkSession.builder.appName("SimpleDeltaDeleter")
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
    def create_sample_data(self, delta_path: str):
        """Create sample Delta table for testing."""
        sample_data = [
            (1, 'John', 'Engineering', 90000, True),
            (2, 'Jane', 'Marketing', 75000, True), 
            (3, 'Mike', 'Finance', 80000, True),
            (4, 'Sarah', 'Engineering', 95000, True),
            (5, 'Tom', 'HR', 70000, False),
            (6, 'Lisa', 'Marketing', 72000, True)
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("active", BooleanType(), True)
        ])
        
        df = self.spark.createDataFrame(sample_data, schema)
        df.write.format("delta").mode("overwrite").save(delta_path)
        
        print(f"✅ Created Delta table at: {delta_path}")
        df.show()
        return df
    
    def delete_by_ids_from_dataframe(self, delta_path: str, ids_df):
        """Delete rows where ID matches DataFrame values."""
        print("\n🗑️  Deleting by IDs from DataFrame...")
        
        # Load Delta table
        delta_table = DeltaTable.forPath(self.spark, delta_path)
        
        # Get IDs from DataFrame
        ids_list = [row.id for row in ids_df.collect()]
        print(f"IDs to delete: {ids_list}")
        
        # Delete using WHERE clause with DataFrame values
        where_condition = col("id").isin(ids_list)
        delta_table.delete(where_condition)
        
        print("✅ Deletion completed")
        delta_table.toDF().show()
    
    def delete_by_department_from_dataframe(self, delta_path: str, dept_df):
        """Delete rows where department matches DataFrame values."""
        print("\n🗑️  Deleting by departments from DataFrame...")
        
        delta_table = DeltaTable.forPath(self.spark, delta_path)
        
        # Get departments from DataFrame
        departments = [row.department for row in dept_df.collect()]
        print(f"Departments to delete: {departments}")
        
        # Delete using WHERE clause
        where_condition = col("department").isin(departments)
        delta_table.delete(where_condition)
        
        print("✅ Deletion completed")
        delta_table.toDF().show()
    
    def delete_by_conditions_from_dataframe(self, delta_path: str, conditions_df):
        """Delete rows using complex conditions from DataFrame."""
        print("\n🗑️  Deleting by complex conditions from DataFrame...")
        
        delta_table = DeltaTable.forPath(self.spark, delta_path)
        
        # Build conditions from DataFrame rows
        deletion_conditions = []
        for row in conditions_df.collect():
            # Example: delete where department=X AND salary >= Y
            condition = (col("department") == lit(row.department)) & \
                       (col("salary") >= lit(row.min_salary))
            deletion_conditions.append(condition)
        
        # Combine conditions with OR
        if deletion_conditions:
            final_condition = deletion_conditions[0]
            for cond in deletion_conditions[1:]:
                final_condition = final_condition | cond
            
            # Show what will be deleted
            to_delete = delta_table.toDF().filter(final_condition)
            print("Records to delete:")
            to_delete.show()
            
            # Perform deletion
            delta_table.delete(final_condition)
            
            print("✅ Deletion completed")
            delta_table.toDF().show()
    
    def delete_by_join_from_dataframe(self, delta_path: str, criteria_df):
        """Delete rows using join-like conditions with DataFrame."""
        print("\n🗑️  Deleting by join conditions from DataFrame...")
        
        delta_table = DeltaTable.forPath(self.spark, delta_path)
        table_df = delta_table.toDF()
        
        # Find matching records through join
        matches = table_df.join(criteria_df, 
                               (table_df.name == criteria_df.name) & 
                               (table_df.department == criteria_df.dept), 
                               "inner")
        
        # Get IDs of matching records
        ids_to_delete = [row.id for row in matches.select("id").collect()]
        print(f"Matching IDs to delete: {ids_to_delete}")
        
        if ids_to_delete:
            where_condition = col("id").isin(ids_to_delete)
            delta_table.delete(where_condition)
            
            print("✅ Deletion completed")
            delta_table.toDF().show()
    
    def cleanup(self):
        """Stop Spark session."""
        self.spark.stop()


def run_examples():
    """Run simple Delta deletion examples."""
    print("🚀 Simple Delta Table Deletion Examples")
    print("=" * 50)
    
    deleter = SimpleDeltaDeleter()
    delta_path = "sample_delta_table"
    
    try:
        # Example 1: Delete by IDs from DataFrame
        print("\n1. DELETE BY IDS FROM DATAFRAME")
        print("-" * 40)
        
        deleter.create_sample_data(delta_path)
        
        # Create DataFrame with IDs to delete
        ids_data = [(2,), (4,)]
        ids_schema = StructType([StructField("id", IntegerType(), True)])
        ids_df = deleter.spark.createDataFrame(ids_data, ids_schema)
        
        deleter.delete_by_ids_from_dataframe(delta_path, ids_df)
        
        # Example 2: Delete by departments from DataFrame
        print("\n2. DELETE BY DEPARTMENTS FROM DATAFRAME") 
        print("-" * 40)
        
        deleter.create_sample_data(delta_path)  # Reset
        
        # Create DataFrame with departments to delete
        dept_data = [('Marketing',), ('HR',)]
        dept_schema = StructType([StructField("department", StringType(), True)])
        dept_df = deleter.spark.createDataFrame(dept_data, dept_schema)
        
        deleter.delete_by_department_from_dataframe(delta_path, dept_df)
        
        # Example 3: Delete by complex conditions from DataFrame
        print("\n3. DELETE BY COMPLEX CONDITIONS FROM DATAFRAME")
        print("-" * 40)
        
        deleter.create_sample_data(delta_path)  # Reset
        
        # Create DataFrame with deletion criteria
        conditions_data = [
            ('Engineering', 90000),  # Delete Engineering with salary >= 90000
            ('Finance', 75000)       # Delete Finance with salary >= 75000
        ]
        conditions_schema = StructType([
            StructField("department", StringType(), True),
            StructField("min_salary", IntegerType(), True)
        ])
        conditions_df = deleter.spark.createDataFrame(conditions_data, conditions_schema)
        
        print("Deletion criteria:")
        conditions_df.show()
        
        deleter.delete_by_conditions_from_dataframe(delta_path, conditions_df)
        
        # Example 4: Delete by join conditions from DataFrame
        print("\n4. DELETE BY JOIN CONDITIONS FROM DATAFRAME")
        print("-" * 40)
        
        deleter.create_sample_data(delta_path)  # Reset
        
        # Create DataFrame with join criteria
        join_data = [
            ('John', 'Engineering'),
            ('Lisa', 'Marketing')
        ]
        join_schema = StructType([
            StructField("name", StringType(), True),
            StructField("dept", StringType(), True)
        ])
        join_df = deleter.spark.createDataFrame(join_data, join_schema)
        
        print("Join criteria:")
        join_df.show()
        
        deleter.delete_by_join_from_dataframe(delta_path, join_df)
        
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        deleter.cleanup()


def quick_delta_delete_pattern():
    """Show the quickest pattern for Delta deletion with DataFrame."""
    
    pattern_code = '''
# QUICK PATTERN: Delete from Delta table using DataFrame WHERE clause

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import *

# Setup Spark with Delta
builder = SparkSession.builder.appName("QuickDeltaDelete")
builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load Delta table
delta_table = DeltaTable.forPath(spark, "path/to/delta/table")

# Create DataFrame with deletion criteria (example: IDs to delete)
criteria_data = [(1,), (3,), (5,)]
criteria_df = spark.createDataFrame(criteria_data, ["id"])

# Extract values for WHERE clause
ids_to_delete = [row.id for row in criteria_df.collect()]

# Delete using DataFrame values in WHERE clause
where_condition = col("id").isin(ids_to_delete)
delta_table.delete(where_condition)

# Alternative: Complex conditions from DataFrame
# conditions_df with columns: department, min_salary
for row in conditions_df.collect():
    condition = (col("department") == row.department) & (col("salary") >= row.min_salary)
    delta_table.delete(condition)
'''
    
    print("\n📋 QUICK DELTA DELETION PATTERN")
    print("=" * 50)
    print(pattern_code)


if __name__ == "__main__":
    run_examples()
    quick_delta_delete_pattern()