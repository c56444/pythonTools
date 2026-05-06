#!/usr/bin/env python3
"""
Delta Table Row Selection using Spark DataFrame Iterations

This script demonstrates how to iterate through a DataFrame and use DataFrame
columns to select rows from Delta tables. Includes various querying patterns,
performance optimizations, and result aggregation techniques.

Required packages:
    pip install pyspark delta-spark pandas

Features:
- DataFrame iteration with Delta table queries
- Multiple selection patterns and criteria
- Batch processing for large DataFrames
- Result aggregation and analysis
- Performance optimizations

Usage:
    python select_delta_rows_with_dataframe.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, collect_list, array_contains, broadcast
from pyspark.sql.types import *
from delta import *
import pandas as pd
import logging
from datetime import datetime, timedelta
import os
import shutil
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DeltaTableSelector:
    """Select rows from Delta tables using DataFrame iterations."""
    
    def __init__(self, app_name: str = "DeltaTableSelector"):
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
            builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            
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
                (1, 'John Doe', 'john.doe@company.com', 'Engineering', 95000, '2023-01-15', True, 'Senior', 'USA'),
                (2, 'Jane Smith', 'jane.smith@company.com', 'Marketing', 72000, '2023-02-01', True, 'Mid', 'USA'),
                (3, 'Mike Johnson', 'mike.johnson@company.com', 'Finance', 78000, '2023-01-20', True, 'Mid', 'Canada'),
                (4, 'Sarah Wilson', 'sarah.wilson@company.com', 'Engineering', 102000, '2023-03-01', True, 'Senior', 'USA'),
                (5, 'Tom Brown', 'tom.brown@company.com', 'HR', 68000, '2023-02-15', True, 'Junior', 'UK'),
                (6, 'Lisa Davis', 'lisa.davis@company.com', 'Marketing', 75000, '2023-01-10', False, 'Mid', 'Canada'),
                (7, 'Chris Miller', 'chris.miller@company.com', 'Finance', 81000, '2023-03-10', True, 'Senior', 'USA'),
                (8, 'Amy Garcia', 'amy.garcia@company.com', 'Engineering', 88000, '2023-02-20', True, 'Mid', 'Mexico'),
                (9, 'David Lee', 'david.lee@company.com', 'HR', 70000, '2023-01-25', True, 'Mid', 'USA'),
                (10, 'Emma Taylor', 'emma.taylor@company.com', 'Finance', 79000, '2023-03-05', True, 'Mid', 'UK'),
                (11, 'Alex Chen', 'alex.chen@company.com', 'Engineering', 115000, '2023-04-01', True, 'Senior', 'USA'),
                (12, 'Maria Rodriguez', 'maria.rodriguez@company.com', 'Marketing', 73000, '2023-04-15', True, 'Mid', 'Mexico'),
                (13, 'James Wilson', 'james.wilson@company.com', 'Finance', 82000, '2023-05-01', True, 'Senior', 'Canada'),
                (14, 'Linda Kim', 'linda.kim@company.com', 'HR', 71000, '2023-05-15', True, 'Mid', 'USA'),
                (15, 'Robert Singh', 'robert.singh@company.com', 'Engineering', 98000, '2023-06-01', True, 'Senior', 'India')
            ]
            
            employee_schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
                StructField("hire_date", StringType(), True),
                StructField("active", BooleanType(), True),
                StructField("level", StringType(), True),
                StructField("country", StringType(), True)
            ])
            
            employees_df = self.spark.createDataFrame(employee_data, employee_schema)
            
            # Write as Delta table
            employees_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(f"{self.delta_tables_path}/employees")
            
            # Create departments lookup table
            dept_data = [
                ('Engineering', 'ENG', 'Technology Division', 25, 80000),
                ('Marketing', 'MKT', 'Sales & Marketing Division', 15, 70000),
                ('Finance', 'FIN', 'Finance & Accounting Division', 12, 75000),
                ('HR', 'HR', 'Human Resources Division', 8, 65000)
            ]
            
            dept_schema = StructType([
                StructField("department", StringType(), True),
                StructField("code", StringType(), True),
                StructField("division", StringType(), True),
                StructField("max_employees", IntegerType(), True),
                StructField("avg_salary", IntegerType(), True)
            ])
            
            departments_df = self.spark.createDataFrame(dept_data, dept_schema)
            departments_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(f"{self.delta_tables_path}/departments")
            
            logger.info("✅ Sample Delta tables created")
            logger.info(f"   Employees table: {employees_df.count()} records")
            logger.info(f"   Departments table: {departments_df.count()} records")
            
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
        """Show current Delta table contents."""
        try:
            delta_df = self.load_delta_table(table_name)
            if delta_df:
                print(f"\n📊 Delta table '{table_name}':")
                delta_df.show(truncate=False)
                print(f"Total records: {delta_df.count()}")
                
        except Exception as e:
            logger.error(f"❌ Failed to show table info: {e}")
    
    def iterate_and_select_by_single_column(self, 
                                          search_df, 
                                          search_column: str,
                                          target_table: str,
                                          target_column: str = None) -> List[Dict]:
        """
        Iterate through DataFrame and select rows from Delta table by single column match.
        
        Args:
            search_df: DataFrame to iterate through
            search_column: Column in search DataFrame
            target_table: Delta table name to search
            target_column: Column in target table (defaults to search_column)
        """
        try:
            if target_column is None:
                target_column = search_column
                
            logger.info(f"🔍 Iterating through DataFrame and selecting from {target_table}")
            logger.info(f"   Matching {search_column} -> {target_column}")
            
            target_df = self.load_delta_table(target_table)
            if target_df is None:
                return []
            
            results = []
            
            print(f"\nSearch criteria DataFrame:")
            search_df.show()
            
            # Iterate through each row in the search DataFrame
            for row in search_df.collect():
                search_value = row[search_column]
                print(f"\n🔎 Searching for {target_column} = '{search_value}'")
                
                # Select matching rows from Delta table
                matching_rows = target_df.filter(col(target_column) == lit(search_value))
                
                if matching_rows.count() > 0:
                    print(f"✅ Found {matching_rows.count()} matching rows:")
                    matching_rows.show()
                    
                    # Collect results
                    for match in matching_rows.collect():
                        result = {
                            'search_value': search_value,
                            'search_column': search_column,
                            'target_column': target_column,
                            'matched_row': match.asDict()
                        }
                        results.append(result)
                else:
                    print("❌ No matching rows found")
            
            logger.info(f"✅ Selection completed. Found {len(results)} total matches")
            return results
            
        except Exception as e:
            logger.error(f"❌ Failed to iterate and select: {e}")
            return []
    
    def iterate_and_select_by_multiple_columns(self, 
                                             search_df, 
                                             column_mapping: Dict[str, str],
                                             target_table: str) -> List[Dict]:
        """
        Iterate through DataFrame and select rows using multiple column conditions.
        
        Args:
            search_df: DataFrame to iterate through
            column_mapping: Dict mapping search_column -> target_column
            target_table: Delta table name to search
        """
        try:
            logger.info(f"🔍 Multi-column selection from {target_table}")
            logger.info(f"   Column mapping: {column_mapping}")
            
            target_df = self.load_delta_table(target_table)
            if target_df is None:
                return []
            
            results = []
            
            print(f"\nSearch criteria DataFrame:")
            search_df.show()
            
            # Iterate through each row in the search DataFrame
            for idx, row in enumerate(search_df.collect()):
                print(f"\n🔎 Row {idx + 1} - Multi-column search:")
                
                # Build conditions for this row
                conditions = []
                search_criteria = {}
                
                for search_col, target_col in column_mapping.items():
                    if search_col in row.asDict():
                        search_value = row[search_col]
                        search_criteria[search_col] = search_value
                        conditions.append(col(target_col) == lit(search_value))
                        print(f"   {target_col} = '{search_value}'")
                
                if conditions:
                    # Combine conditions with AND
                    combined_condition = conditions[0]
                    for condition in conditions[1:]:
                        combined_condition = combined_condition & condition
                    
                    # Select matching rows
                    matching_rows = target_df.filter(combined_condition)
                    match_count = matching_rows.count()
                    
                    if match_count > 0:
                        print(f"✅ Found {match_count} matching rows:")
                        matching_rows.show()
                        
                        # Collect results
                        for match in matching_rows.collect():
                            result = {
                                'row_index': idx,
                                'search_criteria': search_criteria,
                                'column_mapping': column_mapping,
                                'matched_row': match.asDict()
                            }
                            results.append(result)
                    else:
                        print("❌ No matching rows found")
                else:
                    print("⚠️  No valid search criteria found for this row")
            
            logger.info(f"✅ Multi-column selection completed. Found {len(results)} total matches")
            return results
            
        except Exception as e:
            logger.error(f"❌ Failed to perform multi-column selection: {e}")
            return []
    
    def iterate_and_select_with_ranges(self, 
                                     search_df, 
                                     range_column: str,
                                     target_table: str,
                                     target_column: str,
                                     range_type: str = "between") -> List[Dict]:
        """
        Iterate through DataFrame and select rows using range conditions.
        
        Args:
            search_df: DataFrame with range criteria (min_value, max_value columns expected)
            range_column: Column name for range values in search DataFrame
            target_table: Delta table name to search
            target_column: Column in target table to apply range filter
            range_type: Type of range ('between', 'greater_than', 'less_than')
        """
        try:
            logger.info(f"🔍 Range-based selection from {target_table}")
            logger.info(f"   Range type: {range_type}")
            
            target_df = self.load_delta_table(target_table)
            if target_df is None:
                return []
            
            results = []
            
            print(f"\nRange criteria DataFrame:")
            search_df.show()
            
            # Iterate through each row in the search DataFrame
            for idx, row in enumerate(search_df.collect()):
                row_dict = row.asDict()
                print(f"\n🔎 Row {idx + 1} - Range search:")
                
                if range_type == "between" and 'min_value' in row_dict and 'max_value' in row_dict:
                    min_val = row_dict['min_value']
                    max_val = row_dict['max_value']
                    print(f"   {target_column} BETWEEN {min_val} AND {max_val}")
                    
                    condition = (col(target_column) >= lit(min_val)) & (col(target_column) <= lit(max_val))
                    
                elif range_type == "greater_than" and range_column in row_dict:
                    threshold = row_dict[range_column]
                    print(f"   {target_column} > {threshold}")
                    
                    condition = col(target_column) > lit(threshold)
                    
                elif range_type == "less_than" and range_column in row_dict:
                    threshold = row_dict[range_column]
                    print(f"   {target_column} < {threshold}")
                    
                    condition = col(target_column) < lit(threshold)
                else:
                    print("⚠️  Invalid range criteria for this row")
                    continue
                
                # Apply the range condition
                matching_rows = target_df.filter(condition)
                match_count = matching_rows.count()
                
                if match_count > 0:
                    print(f"✅ Found {match_count} matching rows:")
                    matching_rows.show()
                    
                    # Collect results
                    for match in matching_rows.collect():
                        result = {
                            'row_index': idx,
                            'range_criteria': row_dict,
                            'range_type': range_type,
                            'matched_row': match.asDict()
                        }
                        results.append(result)
                else:
                    print("❌ No matching rows found")
            
            logger.info(f"✅ Range selection completed. Found {len(results)} total matches")
            return results
            
        except Exception as e:
            logger.error(f"❌ Failed to perform range selection: {e}")
            return []
    
    def batch_iterate_and_select(self, 
                               search_df, 
                               search_column: str,
                               target_table: str,
                               target_column: str = None,
                               batch_size: int = 100) -> List[Dict]:
        """
        Batch process DataFrame iteration for better performance with large DataFrames.
        
        Args:
            search_df: DataFrame to iterate through
            search_column: Column in search DataFrame
            target_table: Delta table name to search
            target_column: Column in target table (defaults to search_column)
            batch_size: Number of rows to process in each batch
        """
        try:
            if target_column is None:
                target_column = search_column
                
            logger.info(f"🔍 Batch processing DataFrame iteration (batch_size={batch_size})")
            
            target_df = self.load_delta_table(target_table)
            if target_df is None:
                return []
            
            results = []
            total_rows = search_df.count()
            
            print(f"\nProcessing {total_rows} rows in batches of {batch_size}")
            
            # Process in batches
            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                print(f"\n📦 Processing batch: rows {batch_start + 1} to {batch_end}")
                
                # Get batch of search values
                batch_df = search_df.limit(batch_size).offset(batch_start)
                search_values = [row[search_column] for row in batch_df.collect()]
                
                if search_values:
                    print(f"   Search values: {search_values}")
                    
                    # Single query for the entire batch
                    batch_condition = col(target_column).isin(search_values)
                    matching_rows = target_df.filter(batch_condition)
                    
                    match_count = matching_rows.count()
                    print(f"✅ Found {match_count} matching rows for this batch")
                    
                    if match_count > 0:
                        matching_rows.show()
                        
                        # Collect results
                        for match in matching_rows.collect():
                            result = {
                                'batch_start': batch_start,
                                'batch_end': batch_end,
                                'search_column': search_column,
                                'target_column': target_column,
                                'matched_row': match.asDict()
                            }
                            results.append(result)
            
            logger.info(f"✅ Batch processing completed. Found {len(results)} total matches")
            return results
            
        except Exception as e:
            logger.error(f"❌ Failed to perform batch selection: {e}")
            return []
    
    def iterate_and_join_select(self, 
                              search_df, 
                              target_table: str,
                              join_columns: List[str]) -> Optional[object]:
        """
        Use DataFrame join instead of iteration for better performance.
        
        Args:
            search_df: DataFrame with search criteria
            target_table: Delta table name to search
            join_columns: List of columns to join on
        """
        try:
            logger.info(f"🔗 Join-based selection from {target_table}")
            logger.info(f"   Join columns: {join_columns}")
            
            target_df = self.load_delta_table(target_table)
            if target_df is None:
                return None
            
            print(f"\nSearch DataFrame:")
            search_df.show()
            
            print(f"\nTarget table sample:")
            target_df.show(5)
            
            # Perform join instead of iteration
            join_conditions = []
            for col_name in join_columns:
                join_conditions.append(search_df[col_name] == target_df[col_name])
            
            # Combine join conditions
            if join_conditions:
                combined_condition = join_conditions[0]
                for condition in join_conditions[1:]:
                    combined_condition = combined_condition & condition
                
                # Perform the join
                result_df = search_df.join(target_df, combined_condition, "inner")
                
                match_count = result_df.count()
                print(f"\n✅ Join completed. Found {match_count} matching rows:")
                
                if match_count > 0:
                    result_df.show()
                
                logger.info(f"✅ Join-based selection completed successfully")
                return result_df
            else:
                logger.warning("No join columns specified")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to perform join selection: {e}")
            return None
    
    def iterate_with_complex_conditions(self, 
                                      search_df, 
                                      target_table: str,
                                      condition_builder) -> List[Dict]:
        """
        Iterate through DataFrame and apply custom complex conditions.
        
        Args:
            search_df: DataFrame to iterate through
            target_table: Delta table name to search
            condition_builder: Function that takes a row and returns a Spark condition
        """
        try:
            logger.info(f"🔍 Complex condition-based selection from {target_table}")
            
            target_df = self.load_delta_table(target_table)
            if target_df is None:
                return []
            
            results = []
            
            print(f"\nSearch criteria DataFrame:")
            search_df.show()
            
            # Iterate through each row
            for idx, row in enumerate(search_df.collect()):
                print(f"\n🔎 Row {idx + 1} - Complex condition search")
                
                try:
                    # Build custom condition using the provided function
                    condition = condition_builder(row)
                    
                    if condition is not None:
                        matching_rows = target_df.filter(condition)
                        match_count = matching_rows.count()
                        
                        if match_count > 0:
                            print(f"✅ Found {match_count} matching rows:")
                            matching_rows.show()
                            
                            # Collect results
                            for match in matching_rows.collect():
                                result = {
                                    'row_index': idx,
                                    'search_row': row.asDict(),
                                    'matched_row': match.asDict()
                                }
                                results.append(result)
                        else:
                            print("❌ No matching rows found")
                    else:
                        print("⚠️  Invalid condition for this row")
                        
                except Exception as condition_error:
                    print(f"❌ Error building condition for row {idx + 1}: {condition_error}")
                    continue
            
            logger.info(f"✅ Complex condition selection completed. Found {len(results)} total matches")
            return results
            
        except Exception as e:
            logger.error(f"❌ Failed to perform complex condition selection: {e}")
            return []
    
    def aggregate_selection_results(self, results: List[Dict]) -> Dict[str, Any]:
        """Aggregate and analyze selection results."""
        try:
            if not results:
                return {"total_matches": 0}
            
            aggregation = {
                "total_matches": len(results),
                "unique_search_values": set(),
                "matches_per_search": {},
                "target_columns_found": set()
            }
            
            for result in results:
                # Track unique search values
                if 'search_value' in result:
                    search_val = result['search_value']
                    aggregation["unique_search_values"].add(search_val)
                    
                    # Count matches per search value
                    if search_val not in aggregation["matches_per_search"]:
                        aggregation["matches_per_search"][search_val] = 0
                    aggregation["matches_per_search"][search_val] += 1
                
                # Track columns found in results
                if 'matched_row' in result:
                    matched_row = result['matched_row']
                    aggregation["target_columns_found"].update(matched_row.keys())
            
            # Convert sets to lists for display
            aggregation["unique_search_values"] = list(aggregation["unique_search_values"])
            aggregation["target_columns_found"] = list(aggregation["target_columns_found"])
            
            return aggregation
            
        except Exception as e:
            logger.error(f"❌ Failed to aggregate results: {e}")
            return {"error": str(e)}
    
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


def demonstrate_delta_dataframe_selections():
    """Demonstrate various Delta table selection methods using DataFrame iterations."""
    
    print("=" * 100)
    print("🚀 DELTA TABLE SELECTION USING DATAFRAME ITERATIONS")
    print("=" * 100)
    
    # Initialize the selector
    selector = DeltaTableSelector("Delta_DataFrame_Selection_Demo")
    
    # Initialize Spark with Delta
    if not selector.initialize_spark_with_delta():
        print("❌ Failed to initialize Spark with Delta")
        return
    
    try:
        # Create sample Delta tables
        if not selector.create_sample_delta_tables():
            print("❌ Failed to create sample tables")
            return
        
        # Show initial tables
        selector.show_delta_table_info("employees")
        selector.show_delta_table_info("departments")
        
        # Example 1: Single column iteration and selection
        print("\n" + "="*80)
        print("1. SINGLE COLUMN ITERATION AND SELECTION")
        print("="*80)
        
        # Create search DataFrame with department names
        dept_search_data = [('Engineering',), ('Marketing',), ('Finance',)]
        dept_search_schema = StructType([StructField("dept_name", StringType(), True)])
        dept_search_df = selector.spark.createDataFrame(dept_search_data, dept_search_schema)
        
        results1 = selector.iterate_and_select_by_single_column(
            dept_search_df, "dept_name", "employees", "department"
        )
        
        print(f"\n📊 Results Summary: {len(results1)} matches found")
        
        # Example 2: Multiple column iteration and selection
        print("\n" + "="*80)
        print("2. MULTIPLE COLUMN ITERATION AND SELECTION")
        print("="*80)
        
        # Create search DataFrame with multiple criteria
        multi_search_data = [
            ('Engineering', 'Senior'),
            ('Marketing', 'Mid'),
            ('Finance', 'Senior')
        ]
        multi_search_schema = StructType([
            StructField("department", StringType(), True),
            StructField("level", StringType(), True)
        ])
        multi_search_df = selector.spark.createDataFrame(multi_search_data, multi_search_schema)
        
        column_mapping = {'department': 'department', 'level': 'level'}
        results2 = selector.iterate_and_select_by_multiple_columns(
            multi_search_df, column_mapping, "employees"
        )
        
        print(f"\n📊 Results Summary: {len(results2)} matches found")
        
        # Example 3: Range-based iteration and selection
        print("\n" + "="*80)
        print("3. RANGE-BASED ITERATION AND SELECTION")
        print("="*80)
        
        # Create range search DataFrame
        range_search_data = [
            (80000, 100000),  # Salary between 80k and 100k
            (70000, 80000),   # Salary between 70k and 80k
            (100000, 120000)  # Salary between 100k and 120k
        ]
        range_search_schema = StructType([
            StructField("min_value", IntegerType(), True),
            StructField("max_value", IntegerType(), True)
        ])
        range_search_df = selector.spark.createDataFrame(range_search_data, range_search_schema)
        
        results3 = selector.iterate_and_select_with_ranges(
            range_search_df, "salary", "employees", "salary", "between"
        )
        
        print(f"\n📊 Results Summary: {len(results3)} matches found")
        
        # Example 4: Batch processing for performance
        print("\n" + "="*80)
        print("4. BATCH PROCESSING ITERATION")
        print("="*80)
        
        # Create larger search DataFrame for batch demo
        batch_search_data = [(i,) for i in range(1, 16)]  # IDs 1-15
        batch_search_schema = StructType([StructField("emp_id", IntegerType(), True)])
        batch_search_df = selector.spark.createDataFrame(batch_search_data, batch_search_schema)
        
        results4 = selector.batch_iterate_and_select(
            batch_search_df, "emp_id", "employees", "id", batch_size=5
        )
        
        print(f"\n📊 Results Summary: {len(results4)} matches found")
        
        # Example 5: Join-based selection (more efficient than iteration)
        print("\n" + "="*80)
        print("5. JOIN-BASED SELECTION (HIGH PERFORMANCE)")
        print("="*80)
        
        # Create DataFrame for join
        join_search_data = [
            ('Engineering',),
            ('Marketing',),
            ('Finance',)
        ]
        join_search_schema = StructType([StructField("department", StringType(), True)])
        join_search_df = selector.spark.createDataFrame(join_search_data, join_search_schema)
        
        result_df = selector.iterate_and_join_select(
            join_search_df, "employees", ["department"]
        )
        
        if result_df:
            print(f"\n📊 Join Results: {result_df.count()} matches found")
        
        # Example 6: Complex condition iteration
        print("\n" + "="*80)
        print("6. COMPLEX CONDITION ITERATION")
        print("="*80)
        
        # Create search criteria for complex conditions
        complex_search_data = [
            ('Engineering', 90000, 'USA'),
            ('Marketing', 70000, 'Canada'),
            ('Finance', 75000, 'USA')
        ]
        complex_search_schema = StructType([
            StructField("dept", StringType(), True),
            StructField("min_sal", IntegerType(), True),
            StructField("country", StringType(), True)
        ])
        complex_search_df = selector.spark.createDataFrame(complex_search_data, complex_search_schema)
        
        # Define complex condition builder function
        def complex_condition_builder(row):
            """Build complex condition from DataFrame row."""
            return (col("department") == lit(row.dept)) & \
                   (col("salary") >= lit(row.min_sal)) & \
                   (col("country") == lit(row.country))
        
        results6 = selector.iterate_with_complex_conditions(
            complex_search_df, "employees", complex_condition_builder
        )
        
        print(f"\n📊 Results Summary: {len(results6)} matches found")
        
        # Aggregate all results
        print("\n" + "="*80)
        print("7. RESULTS AGGREGATION AND ANALYSIS")
        print("="*80)
        
        aggregation = selector.aggregate_selection_results(results1)
        print("\nAggregation for Single Column Results:")
        for key, value in aggregation.items():
            print(f"  {key}: {value}")
        
    except Exception as e:
        logger.error(f"❌ Error during demonstration: {e}")
    
    finally:
        # Cleanup
        selector.cleanup_resources()
    
    print("\n" + "="*100)
    print("✅ DELTA TABLE DATAFRAME SELECTION DEMONSTRATION COMPLETED")
    print("="*100)


def show_selection_patterns():
    """Show reusable patterns for Delta table selection with DataFrames."""
    
    print("\n" + "="*80)
    print("📋 REUSABLE DELTA SELECTION PATTERNS")
    print("="*80)
    
    patterns = '''
# Pattern 1: Simple DataFrame iteration selection
def iterate_and_select_simple(spark, search_df, search_col, target_table_path, target_col):
    """Basic iteration pattern for row selection."""
    target_df = spark.read.format("delta").load(target_table_path)
    results = []
    
    for row in search_df.collect():
        search_value = row[search_col]
        matches = target_df.filter(col(target_col) == lit(search_value))
        results.extend(matches.collect())
    
    return results

# Pattern 2: Batch processing for performance
def batch_select_pattern(spark, search_df, search_col, target_table_path, target_col, batch_size=100):
    """Batch processing pattern for better performance."""
    target_df = spark.read.format("delta").load(target_table_path)
    total_rows = search_df.count()
    results = []
    
    for batch_start in range(0, total_rows, batch_size):
        batch_df = search_df.limit(batch_size).offset(batch_start)
        search_values = [row[search_col] for row in batch_df.collect()]
        
        # Single query for batch
        batch_matches = target_df.filter(col(target_col).isin(search_values))
        results.extend(batch_matches.collect())
    
    return results

# Pattern 3: Join-based selection (most efficient)
def join_select_pattern(spark, search_df, target_table_path, join_columns):
    """Join-based pattern - most efficient for large datasets."""
    target_df = spark.read.format("delta").load(target_table_path)
    
    join_conditions = []
    for col_name in join_columns:
        join_conditions.append(search_df[col_name] == target_df[col_name])
    
    combined_condition = join_conditions[0]
    for condition in join_conditions[1:]:
        combined_condition = combined_condition & condition
    
    return search_df.join(target_df, combined_condition, "inner")

# Pattern 4: Complex condition iteration
def complex_condition_select_pattern(spark, search_df, target_table_path, condition_func):
    """Pattern for complex custom conditions."""
    target_df = spark.read.format("delta").load(target_table_path)
    results = []
    
    for row in search_df.collect():
        condition = condition_func(row)  # Custom condition builder
        if condition:
            matches = target_df.filter(condition)
            results.extend(matches.collect())
    
    return results

# Pattern 5: Range-based selection
def range_select_pattern(spark, search_df, target_table_path, range_col, min_col="min_val", max_col="max_val"):
    """Pattern for range-based row selection."""
    target_df = spark.read.format("delta").load(target_table_path)
    results = []
    
    for row in search_df.collect():
        min_val = row[min_col]
        max_val = row[max_col]
        
        condition = (col(range_col) >= lit(min_val)) & (col(range_col) <= lit(max_val))
        matches = target_df.filter(condition)
        results.extend(matches.collect())
    
    return results
    '''
    
    print(patterns)


def show_performance_best_practices():
    """Show performance best practices for DataFrame iteration with Delta tables."""
    
    print("\n" + "="*80)
    print("⚡ PERFORMANCE BEST PRACTICES")
    print("="*80)
    
    practices = """
1. PREFER JOINS OVER ITERATIONS
   - Use DataFrame joins instead of iterating when possible
   - Joins leverage Spark's distributed computing capabilities
   - Much faster for large datasets

2. BATCH PROCESSING
   - Process multiple search values in single queries
   - Use .isin() for multiple value matching
   - Reduces number of Delta table scans

3. BROADCAST SMALL DATAFRAMES
   - Use broadcast() for small search DataFrames (<200MB)
   - Improves join performance significantly
   - Example: broadcast(small_df).join(large_delta_table)

4. OPTIMIZE DELTA TABLES
   - Regular compaction: delta_table.optimize().executeCompaction()
   - Z-ordering: delta_table.optimize().executeZOrderBy("column1", "column2")
   - Vacuum old files: delta_table.vacuum()

5. PARTITION STRATEGY
   - Partition Delta tables by commonly filtered columns
   - Enables partition pruning for faster queries
   - Especially effective for date/time columns

6. CACHE FREQUENTLY ACCESSED TABLES
   - Cache Delta tables that are queried repeatedly
   - Use .cache() or .persist() on DataFrames
   - Monitor memory usage

7. AVOID COLLECT() ON LARGE RESULTS
   - Use .show(), .count(), or .write() instead of .collect()
   - Process results in chunks if collection needed
   - Consider using .sample() for previews

8. USE COLUMN PRUNING
   - Select only needed columns: .select("col1", "col2")
   - Reduces data transfer and memory usage
   - Particularly important for wide tables

9. PREDICATE PUSHDOWN
   - Apply filters as early as possible
   - Delta Lake pushes filters to file level
   - Use specific conditions over generic ones

10. MONITOR AND TUNE
    - Check Spark UI for performance bottlenecks
    - Monitor Delta table metrics
    - Adjust parallelism based on cluster size
    """
    
    print(practices)


def main():
    """Main function to run all demonstrations."""
    
    try:
        # Run main demonstration
        demonstrate_delta_dataframe_selections()
        
        # Show patterns and best practices
        show_selection_patterns()
        show_performance_best_practices()
        
        print("\n🎉 All Delta Lake DataFrame selection examples completed!")
        print("\nKey takeaways:")
        print("• Use joins instead of iterations for better performance")
        print("• Batch process multiple values in single queries")
        print("• Leverage Delta Lake's optimization features")
        print("• Cache frequently accessed tables")
        print("• Monitor performance through Spark UI")
        print("• Apply filters early for predicate pushdown")
        
    except Exception as e:
        logger.error(f"❌ Error in main demonstration: {e}")


if __name__ == "__main__":
    main()