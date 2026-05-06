# SQL Table Row Deletion from DataFrame - Usage Guide

## 🎯 Overview

This guide shows how to delete rows from SQL tables using values from pandas DataFrames. Perfect for bulk deletions, data cleanup, and synchronization operations.

## 🚀 Quick Start

### Basic Setup
```python
import pandas as pd
import sqlite3

# Create DataFrame with values to delete
df = pd.DataFrame({
    'employee_id': [1, 2, 3],
    'reason': ['Resigned', 'Terminated', 'Transferred']
})

# Connect to database
conn = sqlite3.connect('database.db')

# Delete rows where employee ID matches DataFrame values
id_list = df['employee_id'].tolist()
placeholders = ','.join(['?' for _ in id_list])
cursor = conn.cursor()
cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", id_list)
conn.commit()
```

## 📋 Common Patterns

### 1. Delete by Single Column (IDs)
```python
def delete_by_ids(df, id_column, table_name, connection):
    """Delete rows where table ID matches DataFrame values."""
    ids = df[id_column].dropna().unique().tolist()
    
    if not ids:
        return 0
    
    placeholders = ','.join(['?' for _ in ids])
    query = f"DELETE FROM {table_name} WHERE id IN ({placeholders})"
    
    cursor = connection.cursor()
    cursor.execute(query, ids)
    deleted_count = cursor.rowcount
    connection.commit()
    
    return deleted_count

# Usage
df = pd.DataFrame({'emp_id': [100, 101, 102]})
deleted = delete_by_ids(df, 'emp_id', 'employees', conn)
print(f"Deleted {deleted} rows")
```

### 2. Delete by Department/Category
```python
def delete_by_category(df, category_column, table_name, connection):
    """Delete all rows in specified categories."""
    categories = df[category_column].dropna().unique().tolist()
    
    placeholders = ','.join(['?' for _ in categories])
    query = f"DELETE FROM {table_name} WHERE department IN ({placeholders})"
    
    cursor = connection.cursor()
    cursor.execute(query, categories)
    connection.commit()
    
    return cursor.rowcount

# Usage  
df = pd.DataFrame({'dept': ['IT', 'HR']})
deleted = delete_by_category(df, 'dept', 'employees', conn)
```

### 3. Delete by Multiple Criteria
```python
def delete_by_multiple_criteria(df, criteria_mapping, table_name, connection):
    """Delete rows matching multiple column criteria."""
    cursor = connection.cursor()
    total_deleted = 0
    
    for _, row in df.iterrows():
        conditions = []
        params = []
        
        for df_col, table_col in criteria_mapping.items():
            conditions.append(f"{table_col} = ?")
            params.append(row[df_col])
        
        where_clause = " AND ".join(conditions)
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        
        cursor.execute(query, params)
        total_deleted += cursor.rowcount
    
    connection.commit()
    return total_deleted

# Usage
df = pd.DataFrame({
    'name': ['John Doe', 'Jane Smith'],
    'department': ['IT', 'HR']
})

criteria = {'name': 'employee_name', 'department': 'dept'}
deleted = delete_by_multiple_criteria(df, criteria, 'employees', conn)
```

### 4. Safe Preview Before Deletion
```python
def preview_deletion(df, column_name, table_name, connection):
    """Preview which rows will be deleted."""
    values = df[column_name].dropna().unique().tolist()
    
    if not values:
        return pd.DataFrame()
    
    placeholders = ','.join(['?' for _ in values])
    query = f"SELECT * FROM {table_name} WHERE {column_name} IN ({placeholders})"
    
    return pd.read_sql_query(query, connection, params=values)

# Usage
df = pd.DataFrame({'emp_id': [100, 101]})
preview = preview_deletion(df, 'emp_id', 'employees', conn)
print(f"Will delete {len(preview)} rows:")
print(preview[['id', 'name', 'department']])

# Proceed with deletion if preview looks correct
deleted = delete_by_ids(df, 'emp_id', 'employees', conn)
```

### 5. Batch Processing for Large Datasets
```python
def delete_in_batches(df, column_name, table_name, connection, batch_size=1000):
    """Delete large datasets in batches to avoid memory issues."""
    values = df[column_name].dropna().unique().tolist()
    total_deleted = 0
    
    cursor = connection.cursor()
    
    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        placeholders = ','.join(['?' for _ in batch])
        query = f"DELETE FROM {table_name} WHERE {column_name} IN ({placeholders})"
        
        cursor.execute(query, batch)
        batch_deleted = cursor.rowcount
        total_deleted += batch_deleted
        
        print(f"Batch {i//batch_size + 1}: Deleted {batch_deleted} rows")
    
    connection.commit()
    return total_deleted

# Usage
large_df = pd.DataFrame({'id': range(1, 10001)})  # 10,000 IDs
deleted = delete_in_batches(large_df, 'id', 'employees', conn, batch_size=500)
```

## 🔧 Database-Specific Examples

### SQLite
```python
import sqlite3

conn = sqlite3.connect('database.db')
# Use examples above - they're designed for SQLite
conn.close()
```

### SQL Server
```python
from sqlalchemy import create_engine, text

conn_str = "mssql+pymssql://user:password@server/database"
engine = create_engine(conn_str)

# Delete using SQLAlchemy
df = pd.DataFrame({'emp_id': [1, 2, 3]})
ids = df['emp_id'].tolist()

with engine.connect() as conn:
    placeholders = ','.join([f':id_{i}' for i in range(len(ids))])
    query = f"DELETE FROM employees WHERE id IN ({placeholders})"
    params = {f'id_{i}': id_val for i, id_val in enumerate(ids)}
    
    result = conn.execute(text(query), params)
    deleted_count = result.rowcount
    conn.commit()
```

### PostgreSQL
```python
from sqlalchemy import create_engine, text

conn_str = "postgresql://user:password@localhost:5432/database"
engine = create_engine(conn_str)

# Same SQLAlchemy approach as SQL Server
with engine.connect() as conn:
    # ... same code as above
    pass
```

### MySQL
```python
from sqlalchemy import create_engine, text

conn_str = "mysql+mysqlconnector://user:password@localhost/database"
engine = create_engine(conn_str)

# Same SQLAlchemy approach
with engine.connect() as conn:
    # ... same code as above  
    pass
```

## ⚡ Performance Tips

### 1. Use Indexes
```sql
-- Create indexes on columns used in WHERE clauses
CREATE INDEX idx_employee_id ON employees(id);
CREATE INDEX idx_employee_dept ON employees(department);
```

### 2. Batch Large Operations
```python
# Good: Process in batches
for i in range(0, len(large_df), 1000):
    batch_df = large_df.iloc[i:i+1000]
    delete_batch(batch_df)

# Avoid: Processing all at once
delete_all(very_large_df)  # May cause memory issues
```

### 3. Use Transactions
```python
# Wrap operations in transactions
conn.execute("BEGIN TRANSACTION")
try:
    # Perform deletions
    delete_operation_1()
    delete_operation_2()
    conn.execute("COMMIT")
except Exception as e:
    conn.execute("ROLLBACK")
    print(f"Error: {e}")
```

## 🛡️ Security & Safety

### 1. Always Use Parameterized Queries
```python
# ✅ SAFE - Parameterized query
cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))

# ❌ DANGEROUS - String concatenation (SQL injection risk)
cursor.execute(f"DELETE FROM users WHERE id = {user_id}")
```

### 2. Backup Before Bulk Deletions
```sql
-- Create backup table
CREATE TABLE employees_backup AS SELECT * FROM employees;

-- Or export to file
.backup backup_file.db  -- SQLite
```

### 3. Validate Data Before Deletion
```python
# Check for unexpected values
print("Unique values to delete:", df['column'].unique())
print("Data types:", df.dtypes)
print("Null values:", df.isnull().sum())

# Verify counts
preview_df = preview_deletion(df, 'id', 'employees', conn)
print(f"Will delete {len(preview_df)} of {total_rows} rows ({len(preview_df)/total_rows*100:.1f}%)")
```

## 🚨 Common Pitfalls

### 1. Not Handling NULL Values
```python
# ❌ May include NaN values
ids = df['id'].tolist()

# ✅ Remove NaN values first  
ids = df['id'].dropna().tolist()
```

### 2. Forgetting to Commit
```python
# ❌ Changes not saved
cursor.execute("DELETE FROM ...")
# Missing: conn.commit()

# ✅ Properly committed
cursor.execute("DELETE FROM ...")
conn.commit()
```

### 3. No Error Handling
```python
# ✅ With proper error handling
try:
    cursor.execute(query, params)
    conn.commit()
    print(f"Deleted {cursor.rowcount} rows")
except Exception as e:
    conn.rollback()
    print(f"Error: {e}")
```

## 📊 Complete Example Workflow

```python
import pandas as pd
import sqlite3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def complete_deletion_workflow():
    """Complete example of safe deletion workflow."""
    
    # 1. Load data to delete
    df = pd.read_csv('employees_to_delete.csv')
    logger.info(f"Loaded {len(df)} records to delete")
    
    # 2. Validate data
    if df['employee_id'].isnull().any():
        logger.warning("Found NULL employee IDs - cleaning data")
        df = df.dropna(subset=['employee_id'])
    
    # 3. Connect to database
    conn = sqlite3.connect('company.db')
    
    try:
        # 4. Create backup
        conn.execute("CREATE TABLE employees_backup AS SELECT * FROM employees")
        logger.info("Created backup table")
        
        # 5. Preview deletion
        preview_df = preview_deletion(df, 'employee_id', 'employees', conn)
        logger.info(f"Preview: Will delete {len(preview_df)} employees")
        
        # 6. Perform deletion
        deleted_count = delete_by_ids(df, 'employee_id', 'employees', conn)
        logger.info(f"Successfully deleted {deleted_count} employees")
        
        # 7. Verify results
        remaining_count = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
        logger.info(f"Remaining employees: {remaining_count}")
        
    except Exception as e:
        logger.error(f"Error during deletion: {e}")
        conn.rollback()
    finally:
        conn.close()

# Run the workflow
complete_deletion_workflow()
```

## 🏃‍♂️ Running the Examples

```bash
# Install requirements
pip install pandas sqlalchemy pymssql psycopg2-binary mysql-connector-python

# Run comprehensive examples
python delete_sql_rows_from_dataframe.py

# Run simple examples
python simple_sql_deletion.py
```

This guide provides everything you need to safely and efficiently delete SQL table rows using pandas DataFrame values!