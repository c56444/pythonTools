# Spark SQL MERGE Statement Quick Reference

## Overview
The MERGE statement (also known as UPSERT) allows you to update existing records and insert new records in a single atomic operation. It's perfect for data synchronization scenarios.

## Basic Syntax

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON join_condition
WHEN MATCHED THEN 
    UPDATE SET column1 = value1, column2 = value2
WHEN NOT MATCHED THEN 
    INSERT (column1, column2) VALUES (value1, value2)
WHEN NOT MATCHED BY SOURCE THEN 
    DELETE
```

## Common Patterns

### 1. Simple Upsert (Update or Insert All Columns)
```sql
MERGE INTO delta.`/path/to/target/table` AS target
USING source_view AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 2. Conditional Updates
```sql
MERGE INTO delta.`/path/to/target/employees` AS target
USING source_employees AS source
ON target.employee_id = source.employee_id
WHEN MATCHED AND source.salary > target.salary THEN 
    UPDATE SET 
        target.salary = source.salary,
        target.last_updated = current_timestamp()
WHEN NOT MATCHED THEN 
    INSERT *
```

### 3. Multiple Join Columns
```sql
MERGE INTO delta.`/path/to/target/sales` AS target
USING source_sales AS source
ON target.customer_id = source.customer_id 
   AND target.product_id = source.product_id 
   AND target.order_date = source.order_date
WHEN MATCHED THEN 
    UPDATE SET 
        target.quantity = target.quantity + source.quantity,
        target.total_amount = target.total_amount + source.total_amount
WHEN NOT MATCHED THEN INSERT *
```

### 4. Merge with Delete
```sql
MERGE INTO delta.`/path/to/target/customers` AS target
USING source_customers AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.status = 'INACTIVE' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 5. Using Expressions and Functions
```sql
MERGE INTO delta.`/path/to/target/inventory` AS target
USING source_inventory AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.quantity = target.quantity + source.quantity_change,
        target.value = (target.quantity + source.quantity_change) * source.unit_price,
        target.last_updated = current_timestamp()
WHEN NOT MATCHED THEN 
    INSERT (product_id, quantity, value, last_updated)
    VALUES (source.product_id, source.quantity_change, 
            source.quantity_change * source.unit_price, current_timestamp())
```

### 6. Using Subquery as Source
```sql
MERGE INTO delta.`/path/to/target/daily_summary` AS target
USING (
    SELECT 
        customer_id,
        order_date,
        SUM(amount) as total_amount,
        COUNT(*) as order_count
    FROM daily_orders
    WHERE order_date = current_date()
    GROUP BY customer_id, order_date
) AS source
ON target.customer_id = source.customer_id 
   AND target.order_date = source.order_date
WHEN MATCHED THEN 
    UPDATE SET 
        target.total_amount = source.total_amount,
        target.order_count = source.order_count
WHEN NOT MATCHED THEN INSERT *
```

### 7. Delta Table API Alternative
```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/target/table")

delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll()\
 .whenNotMatchedInsertAll()\
 .execute()
```

## Best Practices

### 1. Performance Optimization
- **Choose good partition keys**: Ensure your join columns align with partition columns
- **Use Z-ordering**: Order frequently joined columns for better performance
- **Optimize file sizes**: Use OPTIMIZE command after large merges

### 2. Data Quality
- **Validate join keys**: Ensure join columns have appropriate data types and nullability
- **Handle duplicates**: Source data should be deduplicated on join keys
- **Use constraints**: Add CHECK constraints where appropriate

### 3. Monitoring and Logging
```sql
-- Add audit columns
WHEN MATCHED THEN 
    UPDATE SET 
        target.data_column = source.data_column,
        target.updated_at = current_timestamp(),
        target.updated_by = 'merge_job'
WHEN NOT MATCHED THEN 
    INSERT (id, data_column, created_at, updated_at, created_by, updated_by)
    VALUES (source.id, source.data_column, current_timestamp(), 
            current_timestamp(), 'merge_job', 'merge_job')
```

### 4. Error Handling
```python
try:
    # Execute merge
    spark.sql(merge_statement)
    print(f"Merge completed successfully")
except Exception as e:
    print(f"Merge failed: {str(e)}")
    # Log error details
    raise
```

## Common Use Cases

1. **CDC (Change Data Capture)**: Apply incremental changes from source systems
2. **Data Synchronization**: Keep data warehouses in sync with operational systems
3. **Slowly Changing Dimensions**: Update dimension tables in data warehouses
4. **Event Processing**: Merge event data while avoiding duplicates
5. **Inventory Management**: Update stock levels with transactions
6. **Customer 360**: Merge customer data from multiple sources

## Performance Considerations

1. **File Size**: Larger files generally perform better for merge operations
2. **Partitioning**: Align merge keys with partition columns when possible
3. **Clustering**: Use Z-order clustering on frequently joined columns
4. **Broadcast Joins**: Small source datasets may benefit from broadcast hints
5. **Parallelism**: Adjust Spark parallelism based on cluster size and data volume

## Troubleshooting

### Common Issues:
- **Schema mismatch**: Ensure source and target schemas are compatible
- **Null join keys**: Handle null values in join conditions appropriately  
- **Duplicate keys**: Source should have unique values for join columns
- **Performance**: Large cartesian products can cause slow performance

### Debugging Tips:
```python
# Check merge plan
spark.sql(merge_statement).explain(True)

# Validate source data
source_df.groupBy("join_column").count().filter("count > 1").show()

# Check for nulls
source_df.filter(col("join_column").isNull()).count()
```

## Integration with Spark Ecosystem

### With Streaming:
```python
# Structured streaming with merge
def upsert_to_delta(df, epoch_id):
    df.createOrReplaceTempView("updates")
    spark.sql(merge_statement)

streaming_query = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .load()
    .writeStream
    .foreachBatch(upsert_to_delta)
    .start())
```

### With DataFrames:
```python
# Convert DataFrame operations to merge
source_df.createOrReplaceTempView("source_data")
spark.sql(merge_statement)
```