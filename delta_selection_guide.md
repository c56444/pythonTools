# Delta Table Row Selection Guide using DataFrame Iteration

## Overview

This guide demonstrates how to iterate through a Spark DataFrame and use the DataFrame columns to select rows from Delta Lake tables. This pattern is useful for data retrieval, validation, and analytical workflows where you need to find matching records based on dynamic criteria.

## Key Concepts

### DataFrame Iteration Patterns
- **Row-by-row iteration**: Process each DataFrame row individually
- **Batch processing**: Group multiple criteria for efficient querying
- **Join-based selection**: Most efficient approach for large datasets
- **Range-based selection**: Query using value ranges from DataFrame

### Performance Considerations
- Joins are typically faster than iterations
- Batch processing reduces the number of Delta table scans
- Predicate pushdown optimization in Delta Lake
- Caching strategies for frequently accessed tables

## Installation

```bash
pip install pyspark delta-spark pandas
```

## Quick Start

### Basic Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta import *

# Initialize Spark with Delta Lake
builder = SparkSession.builder.appName("DeltaSelector")
builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

### Load Delta Table

```python
# Load Delta table
delta_table = spark.read.format("delta").load("path/to/delta/table")
```

## Selection Patterns

### 1. Single Column Iteration

Iterate through a DataFrame and select rows where one column matches.

```python
def iterate_single_column_select(search_df, search_col, target_table, target_col):
    """Select rows by iterating through single column values."""
    
    results = []
    
    # Iterate through search DataFrame
    for row in search_df.collect():
        search_value = row[search_col]
        
        # Select matching rows from target table
        matches = target_table.filter(col(target_col) == lit(search_value))
        results.extend(matches.collect())
    
    return results

# Usage example
search_data = [('Engineering',), ('Marketing',), ('Finance',)]
search_df = spark.createDataFrame(search_data, ['department'])

results = iterate_single_column_select(search_df, 'department', delta_table, 'department')
```

### 2. Multiple Column Iteration

Use multiple DataFrame columns as selection criteria.

```python
def iterate_multi_column_select(search_df, column_mapping, target_table):
    """Select rows using multiple column conditions."""
    
    results = []
    
    for row in search_df.collect():
        # Build conditions for this row
        conditions = []
        
        for search_col, target_col in column_mapping.items():
            if search_col in row.asDict():
                search_value = row[search_col]
                conditions.append(col(target_col) == lit(search_value))
        
        if conditions:
            # Combine conditions with AND
            combined_condition = conditions[0]
            for condition in conditions[1:]:
                combined_condition = combined_condition & condition
            
            # Select matching rows
            matches = target_table.filter(combined_condition)
            results.extend(matches.collect())
    
    return results

# Usage example
search_data = [
    ('Engineering', 'Senior', 'USA'),
    ('Marketing', 'Mid', 'Canada')
]
search_df = spark.createDataFrame(search_data, ['department', 'level', 'country'])

column_mapping = {
    'department': 'department',
    'level': 'level', 
    'country': 'country'
}

results = iterate_multi_column_select(search_df, column_mapping, delta_table)
```

### 3. Range-Based Selection

Select rows where target values fall within ranges specified in the DataFrame.

```python
def iterate_range_select(search_df, target_table, range_col):
    """Select rows using range conditions from DataFrame."""
    
    results = []
    
    for row in search_df.collect():
        row_dict = row.asDict()
        
        if 'min_value' in row_dict and 'max_value' in row_dict:
            min_val = row_dict['min_value']
            max_val = row_dict['max_value']
            
            # Build range condition
            condition = (col(range_col) >= lit(min_val)) & (col(range_col) <= lit(max_val))
            
            # Select matching rows
            matches = target_table.filter(condition)
            results.extend(matches.collect())
    
    return results

# Usage example
range_data = [
    (70000, 90000),   # Salary between 70k-90k
    (95000, 120000)   # Salary between 95k-120k
]
search_df = spark.createDataFrame(range_data, ['min_value', 'max_value'])

results = iterate_range_select(search_df, delta_table, 'salary')
```

### 4. Batch Processing (Optimized)

Process multiple values in a single query for better performance.

```python
def batch_select_optimized(search_df, search_col, target_table, target_col):
    """Batch process multiple values in single query."""
    
    # Collect all search values
    search_values = [row[search_col] for row in search_df.collect()]
    
    if search_values:
        # Single query with all values
        condition = col(target_col).isin(search_values)
        matches = target_table.filter(condition)
        return matches.collect()
    
    return []

# Usage example
search_data = [(1,), (3,), (5,), (7,), (9,)]
search_df = spark.createDataFrame(search_data, ['employee_id'])

results = batch_select_optimized(search_df, 'employee_id', delta_table, 'id')
```

### 5. Join-Based Selection (Most Efficient)

Use DataFrame joins instead of iteration for maximum performance.

```python
def join_based_select(search_df, target_table, join_columns):
    """Use DataFrame join for optimal performance."""
    
    # Build join conditions
    join_conditions = []
    for col_name in join_columns:
        join_conditions.append(search_df[col_name] == target_table[col_name])
    
    if join_conditions:
        # Combine join conditions
        combined_condition = join_conditions[0]
        for condition in join_conditions[1:]:
            combined_condition = combined_condition & condition
        
        # Perform join
        result_df = search_df.join(target_table, combined_condition, "inner")
        return result_df
    
    return None

# Usage example
search_data = [('Engineering',), ('Marketing',), ('Finance',)]
search_df = spark.createDataFrame(search_data, ['department'])

result_df = join_based_select(search_df, delta_table, ['department'])
if result_df:
    result_df.show()
```

### 6. Complex Condition Iteration

Use custom condition builders for advanced filtering logic.

```python
def iterate_complex_conditions(search_df, target_table, condition_builder):
    """Select rows using custom complex conditions."""
    
    results = []
    
    for row in search_df.collect():
        # Build custom condition
        condition = condition_builder(row)
        
        if condition is not None:
            matches = target_table.filter(condition)
            results.extend(matches.collect())
    
    return results

# Custom condition builder function
def salary_department_condition_builder(row):
    """Build complex condition based on row data."""
    dept = row.department
    min_salary = row.min_salary
    
    return (col("department") == lit(dept)) & (col("salary") >= lit(min_salary))

# Usage example
search_data = [
    ('Engineering', 90000),
    ('Marketing', 70000),
    ('Finance', 80000)
]
search_df = spark.createDataFrame(search_data, ['department', 'min_salary'])

results = iterate_complex_conditions(search_df, delta_table, salary_department_condition_builder)
```

## Performance Optimization

### 1. Choose the Right Pattern

```python
# For small search DataFrames (< 1000 rows) - Iteration is fine
def small_dataset_pattern(search_df, target_table):
    return iterate_single_column_select(search_df, 'id', target_table, 'id')

# For medium search DataFrames (1000-10000 rows) - Use batch processing
def medium_dataset_pattern(search_df, target_table):
    return batch_select_optimized(search_df, 'id', target_table, 'id')

# For large search DataFrames (> 10000 rows) - Use joins
def large_dataset_pattern(search_df, target_table):
    return join_based_select(search_df, target_table, ['id'])
```

### 2. Broadcast Small DataFrames

```python
from pyspark.sql.functions import broadcast

def optimized_join_select(search_df, target_table, join_columns):
    """Use broadcast for small search DataFrames."""
    
    # Broadcast if search_df is small (< 200MB)
    if search_df.count() < 10000:  # Approximate threshold
        search_df = broadcast(search_df)
    
    return join_based_select(search_df, target_table, join_columns)
```

### 3. Cache Frequently Accessed Tables

```python
def setup_caching(target_table):
    """Cache Delta table for repeated access."""
    
    # Cache the table if it will be queried multiple times
    cached_table = target_table.cache()
    
    # Force materialization
    cached_table.count()
    
    return cached_table

# Usage
cached_delta_table = setup_caching(delta_table)
```

### 4. Batch Processing with Size Limits

```python
def controlled_batch_select(search_df, target_table, search_col, target_col, batch_size=1000):
    """Process large search DataFrames in controlled batches."""
    
    total_rows = search_df.count()
    results = []
    
    for batch_start in range(0, total_rows, batch_size):
        # Get batch of rows
        batch_df = search_df.limit(batch_size).offset(batch_start)
        search_values = [row[search_col] for row in batch_df.collect()]
        
        if search_values:
            # Process batch
            condition = col(target_col).isin(search_values)
            matches = target_table.filter(condition)
            results.extend(matches.collect())
    
    return results
```

## Advanced Patterns

### 1. Result Aggregation

```python
def aggregate_selection_results(search_df, target_table, group_by_col):
    """Aggregate results by specific criteria."""
    
    # Join search criteria with target table
    result_df = join_based_select(search_df, target_table, ['department'])
    
    if result_df:
        # Aggregate results
        aggregated = result_df.groupBy(group_by_col).agg(
            {"salary": "avg", "id": "count"}
        ).withColumnRenamed("avg(salary)", "avg_salary") \
         .withColumnRenamed("count(id)", "employee_count")
        
        return aggregated
    
    return None

# Usage
search_df = spark.createDataFrame([('Engineering',), ('Marketing',)], ['department'])
agg_results = aggregate_selection_results(search_df, delta_table, 'department')
if agg_results:
    agg_results.show()
```

### 2. Conditional Selection Logic

```python
def conditional_selection_iterator(search_df, target_table):
    """Apply different selection logic based on search criteria."""
    
    results = []
    
    for row in search_df.collect():
        search_type = row.search_type
        
        if search_type == "exact_match":
            condition = col("department") == lit(row.value)
        elif search_type == "range":
            condition = (col("salary") >= lit(row.min_val)) & (col("salary") <= lit(row.max_val))
        elif search_type == "contains":
            condition = col("name").contains(row.value)
        else:
            continue  # Skip unknown search types
        
        matches = target_table.filter(condition)
        results.extend(matches.collect())
    
    return results
```

### 3. Time-Based Selection

```python
def time_based_selection(search_df, target_table):
    """Select based on time ranges from search DataFrame."""
    
    results = []
    
    for row in search_df.collect():
        start_date = row.start_date
        end_date = row.end_date
        
        condition = (col("hire_date") >= lit(start_date)) & (col("hire_date") <= lit(end_date))
        matches = target_table.filter(condition)
        results.extend(matches.collect())
    
    return results

# Usage example
time_ranges = [
    ('2023-01-01', '2023-03-31'),  # Q1 hires
    ('2023-04-01', '2023-06-30')   # Q2 hires
]
time_search_df = spark.createDataFrame(time_ranges, ['start_date', 'end_date'])
results = time_based_selection(time_search_df, delta_table)
```

## Best Practices

### 1. Error Handling

```python
def safe_selection_iterator(search_df, target_table, search_col, target_col):
    """Iteration with comprehensive error handling."""
    
    results = []
    errors = []
    
    try:
        for idx, row in enumerate(search_df.collect()):
            try:
                if search_col not in row.asDict():
                    errors.append(f"Row {idx}: Missing column '{search_col}'")
                    continue
                
                search_value = row[search_col]
                if search_value is None:
                    errors.append(f"Row {idx}: Null value in '{search_col}'")
                    continue
                
                matches = target_table.filter(col(target_col) == lit(search_value))
                results.extend(matches.collect())
                
            except Exception as row_error:
                errors.append(f"Row {idx}: {str(row_error)}")
                continue
        
        return results, errors
        
    except Exception as e:
        return [], [f"General error: {str(e)}"]
```

### 2. Monitoring and Logging

```python
import time
import logging

def monitored_selection(search_df, target_table, search_col, target_col):
    """Selection with performance monitoring."""
    
    logger = logging.getLogger(__name__)
    
    start_time = time.time()
    search_count = search_df.count()
    
    logger.info(f"Starting selection for {search_count} search criteria")
    
    results = []
    
    for idx, row in enumerate(search_df.collect()):
        row_start = time.time()
        
        search_value = row[search_col]
        matches = target_table.filter(col(target_col) == lit(search_value))
        match_count = matches.count()
        
        results.extend(matches.collect())
        
        row_time = time.time() - row_start
        logger.debug(f"Row {idx}: {match_count} matches in {row_time:.2f}s")
    
    total_time = time.time() - start_time
    logger.info(f"Selection completed: {len(results)} total matches in {total_time:.2f}s")
    
    return results
```

### 3. Memory Management

```python
def memory_efficient_selection(search_df, target_table, search_col, target_col, max_results=10000):
    """Memory-efficient selection with result limits."""
    
    results = []
    total_processed = 0
    
    for row in search_df.collect():
        if len(results) >= max_results:
            print(f"Reached maximum results limit ({max_results})")
            break
        
        search_value = row[search_col]
        matches = target_table.filter(col(target_col) == lit(search_value))
        
        # Limit matches per iteration
        batch_matches = matches.limit(1000).collect()
        results.extend(batch_matches)
        
        total_processed += 1
        
        if total_processed % 100 == 0:
            print(f"Processed {total_processed} search criteria, {len(results)} results so far")
    
    return results
```

## Common Use Cases

### 1. Data Validation

```python
def validate_data_existence(validation_df, target_table, key_columns):
    """Check if records exist in target table."""
    
    validation_results = []
    
    for row in validation_df.collect():
        # Build validation condition
        conditions = []
        for col_name in key_columns:
            if col_name in row.asDict():
                conditions.append(col(col_name) == lit(row[col_name]))
        
        if conditions:
            combined_condition = conditions[0]
            for condition in conditions[1:]:
                combined_condition = combined_condition & condition
            
            exists = target_table.filter(combined_condition).count() > 0
            
            validation_results.append({
                'validation_data': row.asDict(),
                'exists': exists
            })
    
    return validation_results
```

### 2. Data Enrichment

```python
def enrich_data_from_delta(source_df, lookup_table, lookup_columns, select_columns):
    """Enrich source DataFrame with data from Delta table."""
    
    enriched_results = []
    
    for row in source_df.collect():
        # Build lookup condition
        conditions = []
        for col_name in lookup_columns:
            if col_name in row.asDict():
                conditions.append(col(col_name) == lit(row[col_name]))
        
        if conditions:
            combined_condition = conditions[0]
            for condition in conditions[1:]:
                combined_condition = combined_condition & condition
            
            # Get enrichment data
            enrichment_data = lookup_table.filter(combined_condition).select(*select_columns)
            
            if enrichment_data.count() > 0:
                enrichment_row = enrichment_data.first()
                
                # Combine source and enrichment data
                enriched_record = {**row.asDict(), **enrichment_row.asDict()}
                enriched_results.append(enriched_record)
    
    return enriched_results
```

### 3. Audit Trail Analysis

```python
def analyze_audit_trail(criteria_df, audit_table):
    """Analyze audit trail based on search criteria."""
    
    audit_results = []
    
    for row in criteria_df.collect():
        user_id = row.user_id
        start_date = row.start_date
        end_date = row.end_date
        
        condition = (col("user_id") == lit(user_id)) & \
                   (col("timestamp") >= lit(start_date)) & \
                   (col("timestamp") <= lit(end_date))
        
        audit_records = audit_table.filter(condition)
        record_count = audit_records.count()
        
        if record_count > 0:
            # Analyze audit data
            actions = audit_records.groupBy("action").count().collect()
            
            audit_results.append({
                'user_id': user_id,
                'period': f"{start_date} to {end_date}",
                'total_actions': record_count,
                'action_breakdown': {action.action: action.count for action in actions}
            })
    
    return audit_results
```

## Troubleshooting

### Common Issues and Solutions

1. **Performance Issues with Large Iterations**
   - Solution: Use join-based selection or batch processing
   - Monitor Spark UI for bottlenecks

2. **Memory Errors with Large Results**
   - Solution: Implement result limits and streaming processing
   - Use `.show()` or `.write()` instead of `.collect()`

3. **Column Not Found Errors**
   - Solution: Add column existence checks
   - Use `.asDict().keys()` to inspect available columns

4. **Null Value Handling**
   - Solution: Add null checks before building conditions
   - Use `.isNull()` and `.isNotNull()` filters

5. **Type Conversion Issues**
   - Solution: Ensure data types match between search and target columns
   - Use explicit type casting when necessary

This comprehensive guide provides patterns and best practices for efficiently selecting rows from Delta Lake tables using DataFrame iterations, with emphasis on performance optimization and real-world use cases.