#!/usr/bin/env python3
"""
Simple Delta Table Row Selection using DataFrame Iteration

Quick examples for iterating through a DataFrame and using DataFrame
columns to select rows from Delta tables.

Required packages:
    pip install pyspark delta-spark

Usage:
    python simple_delta_selection.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
from delta import *
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleDeltaSelector:
    """Simple Delta table row selection using DataFrame iterations."""
    
    def __init__(self):
        """Initialize with Delta-enabled Spark session."""
        builder = SparkSession.builder.appName("SimpleDeltaSelector")
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
    def create_sample_data(self, delta_path: str):
        """Create sample Delta table for testing."""
        sample_data = [
            (1, 'John', 'Engineering', 90000, True, 'USA'),
            (2, 'Jane', 'Marketing', 75000, True, 'USA'), 
            (3, 'Mike', 'Finance', 80000, True, 'Canada'),
            (4, 'Sarah', 'Engineering', 95000, True, 'USA'),
            (5, 'Tom', 'HR', 70000, False, 'UK'),
            (6, 'Lisa', 'Marketing', 72000, True, 'Canada')
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("active", BooleanType(), True),
            StructField("country", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(sample_data, schema)
        df.write.format("delta").mode("overwrite").save(delta_path)
        
        print(f"✅ Created Delta table at: {delta_path}")
        df.show()
        return df
    
    def iterate_and_select_by_single_column(self, delta_path: str, search_df, search_col: str, target_col: str):
        """Iterate through search DataFrame and select matching rows from Delta table."""
        print(f"\n🔍 Iterating through DataFrame to select rows where {target_col} matches {search_col}...")
        
        # Load Delta table
        delta_table = self.spark.read.format("delta").load(delta_path)
        
        print("Search criteria:")
        search_df.show()
        
        all_results = []
        
        # Iterate through each row in search DataFrame
        for row in search_df.collect():
            search_value = row[search_col]
            print(f"\n🔎 Searching for {target_col} = '{search_value}'")
            
            # Select matching rows from Delta table
            matches = delta_table.filter(col(target_col) == lit(search_value))
            match_count = matches.count()
            
            if match_count > 0:
                print(f"✅ Found {match_count} matching rows:")
                matches.show()
                all_results.extend(matches.collect())
            else:
                print("❌ No matching rows found")
        
        print(f"\n📊 Total matches found: {len(all_results)}")
        return all_results
    
    def iterate_and_select_by_multiple_columns(self, delta_path: str, search_df, column_mapping: dict):
        """Iterate through search DataFrame using multiple column conditions."""
        print(f"\n🔍 Multi-column iteration and selection...")
        
        delta_table = self.spark.read.format("delta").load(delta_path)
        
        print("Search criteria:")
        search_df.show()
        
        all_results = []
        
        # Iterate through each row
        for idx, row in enumerate(search_df.collect()):
            print(f"\n🔎 Row {idx + 1} - Multi-column search:")
            
            # Build conditions for this row
            conditions = []
            for search_col, target_col in column_mapping.items():
                if search_col in row.asDict():
                    search_value = row[search_col]
                    print(f"   {target_col} = '{search_value}'")
                    conditions.append(col(target_col) == lit(search_value))
            
            if conditions:
                # Combine conditions with AND
                combined_condition = conditions[0]
                for condition in conditions[1:]:
                    combined_condition = combined_condition & condition
                
                # Select matching rows
                matches = delta_table.filter(combined_condition)
                match_count = matches.count()
                
                if match_count > 0:
                    print(f"✅ Found {match_count} matching rows:")
                    matches.show()
                    all_results.extend(matches.collect())
                else:
                    print("❌ No matching rows found")
        
        print(f"\n📊 Total matches found: {len(all_results)}")
        return all_results
    
    def iterate_and_select_by_range(self, delta_path: str, search_df, range_col: str):
        """Iterate through search DataFrame using range conditions."""
        print(f"\n🔍 Range-based iteration and selection...")
        
        delta_table = self.spark.read.format("delta").load(delta_path)
        
        print("Range criteria (expecting min_value and max_value columns):")
        search_df.show()
        
        all_results = []
        
        # Iterate through each row
        for idx, row in enumerate(search_df.collect()):
            row_dict = row.asDict()
            
            if 'min_value' in row_dict and 'max_value' in row_dict:
                min_val = row_dict['min_value']
                max_val = row_dict['max_value']
                
                print(f"\n🔎 Row {idx + 1} - Range search: {range_col} BETWEEN {min_val} AND {max_val}")
                
                # Build range condition
                condition = (col(range_col) >= lit(min_val)) & (col(range_col) <= lit(max_val))
                
                # Select matching rows
                matches = delta_table.filter(condition)
                match_count = matches.count()
                
                if match_count > 0:
                    print(f"✅ Found {match_count} matching rows:")
                    matches.show()
                    all_results.extend(matches.collect())
                else:
                    print("❌ No matching rows found")
            else:
                print(f"⚠️  Row {idx + 1} missing min_value or max_value")
        
        print(f"\n📊 Total matches found: {len(all_results)}")
        return all_results
    
    def batch_select_for_performance(self, delta_path: str, search_df, search_col: str, target_col: str):
        """Use batch processing instead of individual iterations for better performance."""
        print(f"\n⚡ Batch processing for better performance...")
        
        delta_table = self.spark.read.format("delta").load(delta_path)
        
        # Collect all search values at once
        search_values = [row[search_col] for row in search_df.collect()]
        print(f"Search values: {search_values}")
        
        # Single query with all values
        if search_values:
            condition = col(target_col).isin(search_values)
            matches = delta_table.filter(condition)
            match_count = matches.count()
            
            print(f"✅ Batch query found {match_count} matching rows:")
            matches.show()
            
            return matches.collect()
        else:
            print("❌ No search values provided")
            return []
    
    def join_based_selection(self, delta_path: str, search_df, join_columns: list):
        """Use DataFrame join instead of iteration - most efficient approach."""
        print(f"\n🔗 Join-based selection (most efficient)...")
        
        delta_table = self.spark.read.format("delta").load(delta_path)
        
        print("Search DataFrame:")
        search_df.show()
        
        print("Target table sample:")
        delta_table.show(3)
        
        # Perform join
        join_conditions = []
        for col_name in join_columns:
            join_conditions.append(search_df[col_name] == delta_table[col_name])
        
        if join_conditions:
            combined_condition = join_conditions[0]
            for condition in join_conditions[1:]:
                combined_condition = combined_condition & condition
            
            result_df = search_df.join(delta_table, combined_condition, "inner")
            match_count = result_df.count()
            
            print(f"✅ Join found {match_count} matching rows:")
            result_df.show()
            
            return result_df
        else:
            print("❌ No join columns specified")
            return None
    
    def cleanup(self):
        """Stop Spark session."""
        self.spark.stop()


def run_selection_examples():
    """Run simple Delta selection examples."""
    print("🚀 Simple Delta Table Selection Examples")
    print("=" * 50)
    
    selector = SimpleDeltaSelector()
    delta_path = "sample_employees_delta"
    
    try:
        # Create sample data
        selector.create_sample_data(delta_path)
        
        # Example 1: Single column iteration
        print("\n1. SINGLE COLUMN ITERATION")
        print("-" * 30)
        
        # Search by department
        dept_search_data = [('Engineering',), ('Marketing',)]
        dept_search_df = selector.spark.createDataFrame(dept_search_data, ['dept_name'])
        
        results1 = selector.iterate_and_select_by_single_column(
            delta_path, dept_search_df, 'dept_name', 'department'
        )
        
        # Example 2: Multiple column iteration
        print("\n2. MULTIPLE COLUMN ITERATION")
        print("-" * 30)
        
        # Search by department and country
        multi_search_data = [
            ('Engineering', 'USA'),
            ('Marketing', 'Canada')
        ]
        multi_search_df = selector.spark.createDataFrame(multi_search_data, ['department', 'country'])
        
        column_mapping = {'department': 'department', 'country': 'country'}
        results2 = selector.iterate_and_select_by_multiple_columns(
            delta_path, multi_search_df, column_mapping
        )
        
        # Example 3: Range-based iteration
        print("\n3. RANGE-BASED ITERATION")
        print("-" * 30)
        
        # Search by salary ranges
        range_search_data = [
            (70000, 80000),   # 70k-80k salary range
            (90000, 100000)   # 90k-100k salary range
        ]
        range_search_df = selector.spark.createDataFrame(range_search_data, ['min_value', 'max_value'])
        
        results3 = selector.iterate_and_select_by_range(
            delta_path, range_search_df, 'salary'
        )
        
        # Example 4: Batch processing (efficient)
        print("\n4. BATCH PROCESSING (EFFICIENT)")
        print("-" * 30)
        
        # Batch search by IDs
        id_search_data = [(1,), (3,), (5,)]
        id_search_df = selector.spark.createDataFrame(id_search_data, ['emp_id'])
        
        results4 = selector.batch_select_for_performance(
            delta_path, id_search_df, 'emp_id', 'id'
        )
        
        # Example 5: Join-based selection (most efficient)
        print("\n5. JOIN-BASED SELECTION (MOST EFFICIENT)")
        print("-" * 30)
        
        # Join search
        join_search_data = [('Engineering',), ('Finance',)]
        join_search_df = selector.spark.createDataFrame(join_search_data, ['department'])
        
        result_df = selector.join_based_selection(
            delta_path, join_search_df, ['department']
        )
        
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        selector.cleanup()


def quick_selection_patterns():
    """Show quick patterns for Delta table selection."""
    
    patterns = '''
# QUICK PATTERNS: Select from Delta table using DataFrame iteration

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta import *

# Setup Spark with Delta
builder = SparkSession.builder.appName("QuickDeltaSelect")
builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Pattern 1: Simple iteration and selection
def iterate_and_select(search_df, search_col, delta_table_path, target_col):
    delta_table = spark.read.format("delta").load(delta_table_path)
    results = []
    
    for row in search_df.collect():
        search_value = row[search_col]
        matches = delta_table.filter(col(target_col) == lit(search_value))
        results.extend(matches.collect())
    
    return results

# Pattern 2: Batch selection (better performance)
def batch_select(search_df, search_col, delta_table_path, target_col):
    delta_table = spark.read.format("delta").load(delta_table_path)
    search_values = [row[search_col] for row in search_df.collect()]
    
    if search_values:
        matches = delta_table.filter(col(target_col).isin(search_values))
        return matches.collect()
    return []

# Pattern 3: Join-based selection (most efficient)
def join_select(search_df, delta_table_path, join_columns):
    delta_table = spark.read.format("delta").load(delta_table_path)
    
    join_conditions = []
    for col_name in join_columns:
        join_conditions.append(search_df[col_name] == delta_table[col_name])
    
    combined_condition = join_conditions[0]
    for condition in join_conditions[1:]:
        combined_condition = combined_condition & condition
    
    return search_df.join(delta_table, combined_condition, "inner")

# Pattern 4: Range-based selection
def range_select(search_df, delta_table_path, target_col):
    delta_table = spark.read.format("delta").load(delta_table_path)
    results = []
    
    for row in search_df.collect():
        min_val = row['min_value']
        max_val = row['max_value']
        
        condition = (col(target_col) >= lit(min_val)) & (col(target_col) <= lit(max_val))
        matches = delta_table.filter(condition)
        results.extend(matches.collect())
    
    return results
'''
    
    print("\n📋 QUICK SELECTION PATTERNS")
    print("=" * 50)
    print(patterns)


if __name__ == "__main__":
    run_selection_examples()
    quick_selection_patterns()