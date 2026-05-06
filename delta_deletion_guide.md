# Delta Table Row Deletion Guide using Spark DataFrames

## Overview

This guide demonstrates how to delete rows from Delta Lake tables using Spark DataFrame columns in WHERE clauses. Delta Lake provides ACID transactions, time travel, and optimized performance for big data workloads.

## Key Features

### Delta Lake Advantages
- **ACID Transactions**: All operations are atomic, consistent, isolated, and durable
- **Time Travel**: Query historical versions of data and recover from mistakes
- **Schema Evolution**: Add columns without breaking existing queries
- **Optimized Performance**: Automatic file compaction and indexing
- **Concurrent Operations**: Safe concurrent reads and writes

### DataFrame Integration
- Use Spark DataFrame values directly in WHERE clauses
- Complex join-based deletion logic
- Batch processing for large datasets
- Analytical deletion patterns

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
builder = SparkSession.builder.appName("DeltaDeleter")
builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

### Create Sample Delta Table

```python
# Sample data
data = [
    (1, 'John', 'Engineering', 90000, True),
    (2, 'Jane', 'Marketing', 75000, True),
    (3, 'Mike', 'Finance', 80000, True)
]

df = spark.createDataFrame(data, ['id', 'name', 'department', 'salary', 'active'])
df.write.format("delta").mode("overwrite").save("employees_delta")
```

## Deletion Patterns

### 1. Delete by IDs from DataFrame

```python
# Create DataFrame with IDs to delete
ids_data = [(2,), (3,)]
ids_df = spark.createDataFrame(ids_data, ['employee_id'])

# Load Delta table
delta_table = DeltaTable.forPath(spark, "employees_delta")

# Extract IDs and create WHERE condition
ids_list = [row.employee_id for row in ids_df.collect()]
where_condition = col("id").isin(ids_list)

# Perform deletion
delta_table.delete(where_condition)
```

### 2. Delete by Department Filter

```python
# DataFrame with departments to delete
dept_data = [('Marketing',), ('HR',)]
dept_df = spark.createDataFrame(dept_data, ['department'])

# Extract departments
departments = [row.department for row in dept_df.collect()]

# Delete rows
where_condition = col("department").isin(departments)
delta_table.delete(where_condition)
```

### 3. Delete with Complex Conditions

```python
# DataFrame with complex criteria
criteria_data = [
    ('Engineering', 90000),  # Delete Engineering with salary >= 90000
    ('Finance', 80000)       # Delete Finance with salary >= 80000
]
criteria_df = spark.createDataFrame(criteria_data, ['dept', 'min_salary'])

# Build complex conditions
deletion_conditions = []
for row in criteria_df.collect():
    condition = (col("department") == lit(row.dept)) & (col("salary") >= lit(row.min_salary))
    deletion_conditions.append(condition)

# Combine with OR
if deletion_conditions:
    final_condition = deletion_conditions[0]
    for cond in deletion_conditions[1:]:
        final_condition = final_condition | cond
    
    delta_table.delete(final_condition)
```

### 4. Delete by Join Conditions

```python
# DataFrame with join criteria
join_data = [('John', 'Engineering'), ('Jane', 'Marketing')]
join_df = spark.createDataFrame(join_data, ['name', 'dept'])

# Find matching records
table_df = delta_table.toDF()
matches = table_df.join(join_df, 
                       (table_df.name == join_df.name) & 
                       (table_df.department == join_df.dept), 
                       "inner")

# Get IDs and delete
ids_to_delete = [row.id for row in matches.select("id").collect()]
if ids_to_delete:
    where_condition = col("id").isin(ids_to_delete)
    delta_table.delete(where_condition)
```

### 5. Subquery-style Deletion

```python
# Register DataFrames as temp views
table_df.createOrReplaceTempView("employees")
criteria_df.createOrReplaceTempView("criteria")

# Use Spark SQL for complex logic
ids_to_delete_df = spark.sql("""
    SELECT e.id 
    FROM employees e
    WHERE e.department IN (
        SELECT dept FROM criteria WHERE min_salary > 85000
    )
""")

# Convert to deletion
ids_list = [row.id for row in ids_to_delete_df.collect()]
if ids_list:
    where_condition = col("id").isin(ids_list)
    delta_table.delete(where_condition)
```

## Advanced Features

### Time Travel and Recovery

```python
# View table history
delta_table.history().show()

# Query previous version
previous_df = spark.read.format("delta").option("versionAsOf", 0).load("employees_delta")

# Query by timestamp
timestamp_df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("employees_delta")

# Restore to previous version
delta_table.restoreToVersion(0)
```

### Table Optimization

```python
# Compact small files
delta_table.optimize().executeCompaction()

# Z-order optimization for better filtering
delta_table.optimize().executeZOrderBy("department", "salary")

# Vacuum old files (removes files older than retention period)
delta_table.vacuum(168)  # 168 hours = 7 days
```

### Batch Processing Large Deletions

```python
def batch_delete_from_large_dataframe(delta_table, large_df, id_col, batch_size=10000):
    """Process large DataFrame deletions in batches."""
    
    # Collect unique IDs (use distinct to avoid duplicates)
    unique_ids_df = large_df.select(id_col).distinct()
    
    # Process in batches using DataFrame operations
    total_count = unique_ids_df.count()
    
    for offset in range(0, total_count, batch_size):
        # Get batch of IDs
        batch_df = unique_ids_df.limit(batch_size).offset(offset)
        ids_list = [row[id_col] for row in batch_df.collect()]
        
        if ids_list:
            where_condition = col("id").isin(ids_list)
            delta_table.delete(where_condition)
            print(f"Processed batch: {offset + len(ids_list)}/{total_count}")

# Usage
large_ids_df = spark.range(1, 100000).select(col("id").alias("employee_id"))
batch_delete_from_large_dataframe(delta_table, large_ids_df, "employee_id")
```

## Performance Tips

### 1. Optimize DataFrame Operations

```python
# Use broadcast for small DataFrames
from pyspark.sql.functions import broadcast

small_df = spark.createDataFrame([(1,), (2,)], ['id'])
broadcast_df = broadcast(small_df)

# More efficient joins with broadcast
matches = table_df.join(broadcast_df, table_df.id == broadcast_df.id, "inner")
```

### 2. Partition Strategy

```python
# Write partitioned Delta table
df.write.format("delta") \
  .partitionBy("department") \
  .mode("overwrite") \
  .save("employees_partitioned")

# Deletions will be more efficient when filtering by partition column
delta_table.delete(col("department") == "Engineering")
```

### 3. Column Statistics

```python
# Analyze table statistics
spark.sql("ANALYZE TABLE delta.`employees_delta` COMPUTE STATISTICS FOR COLUMNS id, department, salary")

# Check statistics
spark.sql("DESCRIBE DETAIL delta.`employees_delta`").show()
```

## Error Handling and Monitoring

### Safe Deletion Pattern

```python
def safe_delta_delete(delta_table, where_condition, dry_run=True):
    """Safely delete with preview and error handling."""
    
    try:
        # Preview what will be deleted
        table_df = delta_table.toDF()
        to_delete = table_df.filter(where_condition)
        
        print(f"Records to be deleted: {to_delete.count()}")
        to_delete.show(20)
        
        if not dry_run:
            # Perform actual deletion
            delta_table.delete(where_condition)
            print("✅ Deletion completed successfully")
        else:
            print("🔍 Dry run - no actual deletion performed")
            
    except Exception as e:
        print(f"❌ Error during deletion: {e}")
        # Log error details
        # Optionally rollback or alert
```

### Transaction Monitoring

```python
# Monitor Delta table operations
def monitor_delta_operations(delta_table):
    """Monitor Delta table operations and metrics."""
    
    # Get table details
    details = spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`")
    details.show()
    
    # Get operation history
    history = delta_table.history()
    recent_operations = history.limit(10)
    recent_operations.show()
    
    # Check table statistics
    stats = spark.sql(f"DESCRIBE EXTENDED delta.`{delta_path}`")
    stats.show()
```

## Best Practices

1. **Always Test First**: Use dry runs and preview operations
2. **Use Transactions**: Delta Lake handles ACID automatically
3. **Monitor Performance**: Check Spark UI and Delta metrics
4. **Optimize Regularly**: Run compaction and vacuum operations
5. **Handle Large Data**: Use batch processing for large deletions
6. **Leverage Partitioning**: Partition by commonly filtered columns
7. **Use Time Travel**: Keep audit trails and enable recovery
8. **Validate Data**: Check constraints and data quality
9. **Security**: Use proper access controls and audit logging
10. **Documentation**: Document complex deletion logic and criteria

## Common Use Cases

### Data Retention Policies

```python
# Delete old records based on DataFrame criteria
retention_criteria = [
    ('logs', 90),      # Keep logs for 90 days
    ('metrics', 30),   # Keep metrics for 30 days
    ('temp', 1)        # Keep temp data for 1 day
]

criteria_df = spark.createDataFrame(retention_criteria, ['table_type', 'days'])

for row in criteria_df.collect():
    cutoff_date = datetime.now() - timedelta(days=row.days)
    condition = (col("table_type") == row.table_type) & (col("created_date") < cutoff_date)
    delta_table.delete(condition)
```

### Data Quality Cleanup

```python
# Remove records based on data quality rules from DataFrame
quality_rules = [
    ('email', 'is_null'),
    ('salary', 'negative'),
    ('department', 'unknown')
]

rules_df = spark.createDataFrame(quality_rules, ['column', 'rule'])

for row in rules_df.collect():
    if row.rule == 'is_null':
        condition = col(row.column).isNull()
    elif row.rule == 'negative':
        condition = col(row.column) < 0
    elif row.rule == 'unknown':
        condition = col(row.column) == 'unknown'
    
    delta_table.delete(condition)
```

## Troubleshooting

### Common Issues

1. **Large Collect Operations**: Avoid `collect()` on large DataFrames
   - Solution: Use aggregations or batch processing

2. **Memory Issues**: Large deletion operations consume memory
   - Solution: Process in smaller batches

3. **Concurrent Modifications**: Multiple writers to same table
   - Solution: Use Delta Lake's optimistic concurrency control

4. **Performance Degradation**: Too many small files
   - Solution: Regular optimization and compaction

### Debug Techniques

```python
# Enable detailed logging
spark.sparkContext.setLogLevel("INFO")

# Check execution plans
df.explain(True)

# Monitor Spark UI
# Access at http://localhost:4040 during execution

# Profile operations
import time
start_time = time.time()
delta_table.delete(condition)
end_time = time.time()
print(f"Operation took: {end_time - start_time:.2f} seconds")
```

This guide provides comprehensive patterns for deleting rows from Delta Lake tables using Spark DataFrame values in WHERE clauses, with emphasis on performance, safety, and best practices.