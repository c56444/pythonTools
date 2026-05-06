# Spark DataFrame to SQL Table Deletion - Quick Guide

## 🚀 Overview

Delete rows from SQL tables using Apache Spark DataFrames for large-scale, distributed data processing. Perfect for big data scenarios and complex analytics-driven deletions.

## ⚡ Quick Setup

### Install Dependencies
```bash
pip install pyspark pandas sqlalchemy
```

### Basic Spark Session
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQLDeletion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

## 🎯 Common Patterns

### 1. Delete by IDs from Spark DataFrame
```python
from pyspark.sql.types import *

# Create Spark DataFrame with IDs to delete
ids_data = [(1,), (2,), (3,)]
schema = StructType([StructField("employee_id", IntegerType(), True)])
ids_df = spark.createDataFrame(ids_data, schema)

# Extract IDs (safe for small datasets)
ids_to_delete = [row.employee_id for row in ids_df.collect()]

# Delete from SQL
conn = sqlite3.connect('database.db')
placeholders = ','.join(['?' for _ in ids_to_delete])
cursor = conn.cursor()
cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", ids_to_delete)
conn.commit()
```

### 2. Delete by Department/Category
```python
# Spark DataFrame with departments to close
dept_data = [('Marketing',), ('HR',)]
dept_schema = StructType([StructField("department", StringType(), True)])
dept_df = spark.createDataFrame(dept_data, dept_schema)

# Get department list
departments = [row.department for row in dept_df.collect()]

# Delete from SQL
placeholders = ','.join(['?' for _ in departments])
cursor.execute(f"DELETE FROM employees WHERE department IN ({placeholders})", departments)
```

### 3. Analytics-Driven Deletion
```python
# Load SQL data into Spark for analysis
pandas_df = pd.read_sql_query("SELECT * FROM employees", conn)
spark_df = spark.createDataFrame(pandas_df)

# Use Spark to identify records to delete (e.g., outliers)
salary_95th_percentile = spark_df.approxQuantile("salary", [0.95], 0.01)[0]
high_earners = spark_df.filter(col("salary") >= salary_95th_percentile)

# Get IDs to delete
ids_to_delete = [row.id for row in high_earners.select("id").collect()]

# Delete from SQL
placeholders = ','.join(['?' for _ in ids_to_delete])
cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", ids_to_delete)
```

### 4. Spark SQL Analysis
```python
# Create temporary view
employees_df.createOrReplaceTempView("employees_analysis")

# Complex SQL analysis
result_df = spark.sql("""
    SELECT id 
    FROM employees_analysis 
    WHERE department IN (
        SELECT department 
        FROM employees_analysis 
        GROUP BY department 
        HAVING AVG(salary) > 100000
    )
""")

# Extract IDs and delete
ids_to_delete = [row.id for row in result_df.collect()]
# ... perform SQL deletion
```

### 5. DataFrame Join for Deletion
```python
# Current employees in Spark
current_employees = spark.createDataFrame(pandas_df)

# Termination list as Spark DataFrame
termination_data = [
    ('alice@company.com', 'Resigned'),
    ('bob@company.com', 'Terminated')
]
termination_schema = StructType([
    StructField("email", StringType(), True),
    StructField("reason", StringType(), True)
])
termination_df = spark.createDataFrame(termination_data, termination_schema)

# Join to find employees to delete
employees_to_delete = current_employees.join(
    termination_df, 
    current_employees.email == termination_df.email, 
    "inner"
).select(current_employees.id)

# Extract IDs and delete
ids_to_delete = [row.id for row in employees_to_delete.collect()]
# ... perform SQL deletion
```

## 🔄 Batch Processing for Large Datasets

### Pattern: Batch Processing
```python
def batch_delete_from_spark(spark_df, id_column, sql_table, sql_connection, batch_size=1000):
    """Process large Spark DataFrame deletions in batches."""
    
    # For very large datasets, consider using mapPartitions or foreachPartition
    # instead of collect() to avoid driver memory issues
    
    all_ids = [row[id_column] for row in spark_df.select(id_column).collect()]
    
    total_deleted = 0
    cursor = sql_connection.cursor()
    
    for i in range(0, len(all_ids), batch_size):
        batch = all_ids[i:i + batch_size]
        placeholders = ','.join(['?' for _ in batch])
        query = f"DELETE FROM {sql_table} WHERE id IN ({placeholders})"
        
        cursor.execute(query, batch)
        batch_deleted = cursor.rowcount
        total_deleted += batch_deleted
        
        print(f"Batch {i//batch_size + 1}: Deleted {batch_deleted} rows")
    
    sql_connection.commit()
    return total_deleted

# Usage
large_df = spark.createDataFrame(large_id_list, schema)
deleted_count = batch_delete_from_spark(large_df, 'id', 'employees', conn, 500)
```

### Alternative: Partition Processing
```python
def process_partition_deletes(partition_iter):
    """Process deletions per partition to avoid collecting all data to driver."""
    import sqlite3
    
    # Create connection per partition
    local_conn = sqlite3.connect('database.db')
    cursor = local_conn.cursor()
    
    ids_in_partition = [row.id for row in partition_iter]
    
    if ids_in_partition:
        placeholders = ','.join(['?' for _ in ids_in_partition])
        cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", ids_in_partition)
        local_conn.commit()
    
    local_conn.close()
    return [len(ids_in_partition)]

# Apply to each partition
deletion_counts = large_df.mapPartitions(process_partition_deletes).collect()
total_deleted = sum(deletion_counts)
```

## 🔗 Database Connections

### SQLite (Simple)
```python
import sqlite3
conn = sqlite3.connect('database.db')
# Use with patterns above
```

### PostgreSQL (JDBC)
```python
# Spark configuration
spark = SparkSession.builder \
    .appName("PostgreSQLDeletion") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.1") \
    .getOrCreate()

# Read PostgreSQL table directly into Spark
employees_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/database") \
    .option("dbtable", "employees") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# Use SQLAlchemy for deletions
from sqlalchemy import create_engine
engine = create_engine("postgresql://user:password@localhost:5432/database")
```

### SQL Server (JDBC)
```python
spark = SparkSession.builder \
    .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:11.2.1.jre8") \
    .getOrCreate()

employees_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://server:1433;databaseName=mydb") \
    .option("dbtable", "employees") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
```

### MySQL (JDBC)
```python
spark = SparkSession.builder \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

employees_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/database") \
    .option("dbtable", "employees") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
```

## ⚠️ Important Considerations

### Memory Management
```python
# ❌ DANGEROUS: Don't collect() large datasets
large_ids = large_df.collect()  # May cause OutOfMemoryError

# ✅ SAFE: Use sampling, aggregation, or partitioned processing
sample_ids = large_df.sample(0.1).collect()  # Sample 10%
id_count = large_df.count()  # Count only
large_df.foreachPartition(process_partition)  # Process per partition
```

### Performance Optimization
```python
# Cache frequently accessed DataFrames
employees_df.cache()

# Use appropriate partitioning
employees_df.repartition(col("department"))

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Monitor with Spark UI at http://localhost:4040
```

### Resource Management
```python
# Always stop Spark session
try:
    # ... your Spark operations
    pass
finally:
    spark.stop()

# Or use context manager pattern
class SparkManager:
    def __enter__(self):
        self.spark = SparkSession.builder.appName("App").getOrCreate()
        return self.spark
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()

# Usage
with SparkManager() as spark:
    # ... use spark here
    pass
```

## 🛡️ Best Practices

### 1. Security
```python
# ✅ Use parameterized queries
cursor.execute("DELETE FROM table WHERE id IN ({})".format(','.join(['?'] * len(ids))), ids)

# ❌ Never use string concatenation
cursor.execute(f"DELETE FROM table WHERE id IN ({','.join(map(str, ids))})")  # SQL injection risk!
```

### 2. Error Handling
```python
try:
    # Spark operations
    ids_df = spark.createDataFrame(data, schema)
    ids_to_delete = [row.id for row in ids_df.collect()]
    
    # SQL operations
    cursor.execute(query, ids_to_delete)
    conn.commit()
    
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    conn.close()
    spark.stop()
```

### 3. Validation
```python
# Validate before deletion
print(f"Records to delete: {ids_df.count()}")
print("Sample records:")
ids_df.show(5)

# Confirm deletion
answer = input("Proceed with deletion? (y/N): ")
if answer.lower() == 'y':
    # Perform deletion
    pass
```

### 4. Logging and Monitoring
```python
import logging

logger = logging.getLogger(__name__)

# Log operations
logger.info(f"Starting deletion of {ids_df.count()} records")
deleted_count = perform_deletion()
logger.info(f"Successfully deleted {deleted_count} records")

# Monitor Spark metrics
print(f"Spark UI: http://localhost:4040")
print(f"Application ID: {spark.sparkContext.applicationId}")
```

## 🏃‍♂️ Quick Start Commands

```bash
# Install dependencies
pip install pyspark pandas sqlalchemy

# Run comprehensive examples
python delete_sql_rows_from_spark.py

# Run simple examples
python simple_spark_sql_deletion.py

# Start Spark with specific memory settings
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_DRIVER_PYTHON_OPTS=""
pyspark --driver-memory 2g --executor-memory 2g
```

## 🎯 When to Use Spark vs Pandas

### Use Spark When:
- Dataset > 1GB or doesn't fit in memory
- Need distributed processing across multiple machines
- Complex analytical queries before deletion
- Real-time streaming data processing
- Integration with Hadoop ecosystem (HDFS, Hive, etc.)

### Use Pandas When:
- Dataset < 1GB and fits comfortably in memory  
- Simple deletion operations
- Working on single machine
- Faster development for small datasets
- Integration with Jupyter notebooks

This guide provides everything you need to efficiently delete SQL rows using Spark DataFrames for big data scenarios!