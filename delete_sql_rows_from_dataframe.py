#!/usr/bin/env python3
"""
SQL Table Row Deletion using DataFrame Values

This script deletes rows from SQL tables based on values from pandas DataFrames.
Supports multiple database types and deletion strategies with proper security measures.

Required packages:
    pip install pandas sqlalchemy pymssql psycopg2-binary mysql-connector-python

Supported databases:
- SQLite
- SQL Server  
- PostgreSQL
- MySQL

Usage:
    python delete_sql_rows_from_dataframe.py
"""

import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
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


class SQLTableDeleter:
    """Delete rows from SQL tables using DataFrame values."""
    
    def __init__(self, connection_string: str = None, db_type: str = 'sqlite'):
        """
        Initialize the SQL Table Deleter.
        
        Args:
            connection_string: Database connection string
            db_type: Type of database ('sqlite', 'mssql', 'postgresql', 'mysql')
        """
        self.connection_string = connection_string
        self.db_type = db_type.lower()
        self.engine = None
        self.connection = None
        
    def connect(self):
        """Establish database connection."""
        try:
            if self.db_type == 'sqlite':
                # For SQLite, create a simple connection
                if not self.connection_string:
                    self.connection_string = 'test_database.db'
                self.connection = sqlite3.connect(self.connection_string)
                self.engine = create_engine(f'sqlite:///{self.connection_string}')
            else:
                # For other databases, use SQLAlchemy
                self.engine = create_engine(self.connection_string)
            
            logger.info(f"✅ Connected to {self.db_type.upper()} database")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            return False
    
    def create_sample_table(self, table_name: str = 'employees'):
        """Create a sample table for demonstration."""
        try:
            if self.db_type == 'sqlite':
                cursor = self.connection.cursor()
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        age INTEGER,
                        salary REAL,
                        department TEXT,
                        active BOOLEAN DEFAULT 1,
                        created_date TEXT
                    )
                ''')
                
                # Insert sample data
                sample_data = [
                    (1, 'Alice Johnson', 28, 65000, 'IT', 1, '2024-01-15'),
                    (2, 'Bob Smith', 32, 70000, 'Finance', 1, '2024-02-01'),
                    (3, 'Charlie Brown', 45, 85000, 'HR', 1, '2024-01-20'),
                    (4, 'Diana Prince', 29, 68000, 'IT', 1, '2024-03-01'),
                    (5, 'Eve Wilson', 35, 75000, 'Marketing', 1, '2024-02-15'),
                    (6, 'Frank Miller', 41, 82000, 'Finance', 0, '2024-01-10'),
                    (7, 'Grace Lee', 26, 58000, 'HR', 1, '2024-03-10'),
                    (8, 'Henry Davis', 38, 79000, 'IT', 1, '2024-02-20'),
                    (9, 'Iris Chen', 31, 72000, 'Marketing', 1, '2024-01-25'),
                    (10, 'Jack Wilson', 44, 88000, 'Finance', 1, '2024-03-05')
                ]
                
                cursor.executemany(f'''
                    INSERT OR REPLACE INTO {table_name} 
                    (id, name, age, salary, department, active, created_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', sample_data)
                
                self.connection.commit()
                
            else:
                # For other databases, use SQLAlchemy
                with self.engine.connect() as conn:
                    # Create table (syntax may vary by database)
                    conn.execute(text(f'''
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id INT PRIMARY KEY,
                            name VARCHAR(100) NOT NULL,
                            age INT,
                            salary DECIMAL(10,2),
                            department VARCHAR(50),
                            active BOOLEAN DEFAULT TRUE,
                            created_date DATE
                        )
                    '''))
                    
                    # Insert sample data would go here
                    # (Implementation varies by database type)
            
            logger.info(f"✅ Sample table '{table_name}' created with test data")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create sample table: {e}")
            return False
    
    def get_table_data(self, table_name: str) -> pd.DataFrame:
        """Get current data from the table."""
        try:
            if self.db_type == 'sqlite':
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.connection)
            else:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.engine)
            
            logger.info(f"📊 Retrieved {len(df)} rows from table '{table_name}'")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to retrieve table data: {e}")
            return pd.DataFrame()
    
    def delete_by_single_column(self, 
                               table_name: str, 
                               dataframe: pd.DataFrame, 
                               df_column: str, 
                               table_column: str = None,
                               batch_size: int = 1000) -> int:
        """
        Delete rows from table where table_column matches values in df_column.
        
        Args:
            table_name: SQL table name
            dataframe: DataFrame containing values to delete
            df_column: Column name in DataFrame
            table_column: Column name in SQL table (defaults to df_column)
            batch_size: Number of values to process per batch
            
        Returns:
            Number of rows deleted
        """
        if table_column is None:
            table_column = df_column
            
        try:
            # Get unique values from DataFrame column
            values_to_delete = dataframe[df_column].dropna().unique().tolist()
            
            if not values_to_delete:
                logger.warning(f"No values found in column '{df_column}'")
                return 0
            
            logger.info(f"🗑️  Deleting rows where {table_column} in {len(values_to_delete)} values")
            
            total_deleted = 0
            
            # Process in batches to avoid SQL parameter limits
            for i in range(0, len(values_to_delete), batch_size):
                batch = values_to_delete[i:i + batch_size]
                
                if self.db_type == 'sqlite':
                    # SQLite approach
                    placeholders = ','.join(['?' for _ in batch])
                    query = f"DELETE FROM {table_name} WHERE {table_column} IN ({placeholders})"
                    
                    cursor = self.connection.cursor()
                    cursor.execute(query, batch)
                    deleted_count = cursor.rowcount
                    self.connection.commit()
                    
                else:
                    # SQLAlchemy approach for other databases
                    placeholders = ','.join([f':val_{j}' for j in range(len(batch))])
                    query = f"DELETE FROM {table_name} WHERE {table_column} IN ({placeholders})"
                    
                    params = {f'val_{j}': val for j, val in enumerate(batch)}
                    
                    with self.engine.connect() as conn:
                        result = conn.execute(text(query), params)
                        deleted_count = result.rowcount
                        conn.commit()
                
                total_deleted += deleted_count
                logger.info(f"   Batch {i//batch_size + 1}: Deleted {deleted_count} rows")
            
            logger.info(f"✅ Total rows deleted: {total_deleted}")
            return total_deleted
            
        except Exception as e:
            logger.error(f"❌ Failed to delete by single column: {e}")
            return 0
    
    def delete_by_multiple_columns(self, 
                                  table_name: str, 
                                  dataframe: pd.DataFrame, 
                                  column_mapping: Dict[str, str],
                                  batch_size: int = 500) -> int:
        """
        Delete rows from table using multiple column conditions.
        
        Args:
            table_name: SQL table name
            dataframe: DataFrame containing values to delete
            column_mapping: Dict mapping df_column -> table_column
            batch_size: Number of rows to process per batch
            
        Returns:
            Number of rows deleted
        """
        try:
            # Get relevant columns from DataFrame
            df_columns = list(column_mapping.keys())
            df_subset = dataframe[df_columns].dropna()
            
            if df_subset.empty:
                logger.warning("No complete rows found for deletion")
                return 0
            
            logger.info(f"🗑️  Deleting rows using {len(column_mapping)} columns")
            logger.info(f"   Processing {len(df_subset)} DataFrame rows")
            
            total_deleted = 0
            
            # Process in batches
            for i in range(0, len(df_subset), batch_size):
                batch_df = df_subset.iloc[i:i + batch_size]
                
                # Build WHERE conditions for each row
                where_conditions = []
                params = {}
                param_counter = 0
                
                for idx, row in batch_df.iterrows():
                    row_conditions = []
                    for df_col, table_col in column_mapping.items():
                        param_name = f'p_{param_counter}'
                        row_conditions.append(f"{table_col} = :{param_name}")
                        params[param_name] = row[df_col]
                        param_counter += 1
                    
                    where_conditions.append(f"({' AND '.join(row_conditions)})")
                
                # Combine all conditions with OR
                full_where = ' OR '.join(where_conditions)
                query = f"DELETE FROM {table_name} WHERE {full_where}"
                
                if self.db_type == 'sqlite':
                    # Convert SQLAlchemy-style parameters to SQLite style
                    sqlite_query = query
                    sqlite_params = []
                    for key in sorted(params.keys(), key=lambda x: int(x.split('_')[1])):
                        sqlite_query = sqlite_query.replace(f':{key}', '?', 1)
                        sqlite_params.append(params[key])
                    
                    cursor = self.connection.cursor()
                    cursor.execute(sqlite_query, sqlite_params)
                    deleted_count = cursor.rowcount
                    self.connection.commit()
                    
                else:
                    # SQLAlchemy approach
                    with self.engine.connect() as conn:
                        result = conn.execute(text(query), params)
                        deleted_count = result.rowcount
                        conn.commit()
                
                total_deleted += deleted_count
                logger.info(f"   Batch {i//batch_size + 1}: Deleted {deleted_count} rows")
            
            logger.info(f"✅ Total rows deleted: {total_deleted}")
            return total_deleted
            
        except Exception as e:
            logger.error(f"❌ Failed to delete by multiple columns: {e}")
            return 0
    
    def delete_with_conditions(self, 
                              table_name: str, 
                              dataframe: pd.DataFrame, 
                              conditions: Dict[str, Any]) -> int:
        """
        Delete rows from table with additional conditions beyond DataFrame values.
        
        Args:
            table_name: SQL table name
            dataframe: DataFrame containing values
            conditions: Additional WHERE conditions
            
        Returns:
            Number of rows deleted
        """
        try:
            # This is a more advanced method that combines DataFrame values
            # with additional SQL conditions
            logger.info(f"🗑️  Deleting with additional conditions: {conditions}")
            
            # Example implementation for a common use case
            if 'id_column' in conditions and 'additional_where' in conditions:
                id_column = conditions['id_column']
                additional_where = conditions['additional_where']
                
                ids_to_delete = dataframe[id_column].dropna().unique().tolist()
                
                if not ids_to_delete:
                    return 0
                
                if self.db_type == 'sqlite':
                    placeholders = ','.join(['?' for _ in ids_to_delete])
                    query = f"""
                        DELETE FROM {table_name} 
                        WHERE {id_column} IN ({placeholders}) 
                        AND {additional_where}
                    """
                    
                    cursor = self.connection.cursor()
                    cursor.execute(query, ids_to_delete)
                    deleted_count = cursor.rowcount
                    self.connection.commit()
                    
                else:
                    placeholders = ','.join([f':id_{j}' for j in range(len(ids_to_delete))])
                    query = f"""
                        DELETE FROM {table_name} 
                        WHERE {id_column} IN ({placeholders}) 
                        AND {additional_where}
                    """
                    
                    params = {f'id_{j}': id_val for j, id_val in enumerate(ids_to_delete)}
                    
                    with self.engine.connect() as conn:
                        result = conn.execute(text(query), params)
                        deleted_count = result.rowcount
                        conn.commit()
                
                logger.info(f"✅ Deleted {deleted_count} rows with conditions")
                return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Failed to delete with conditions: {e}")
            return 0
    
    def safe_delete_preview(self, 
                           table_name: str, 
                           dataframe: pd.DataFrame, 
                           df_column: str, 
                           table_column: str = None) -> pd.DataFrame:
        """
        Preview which rows would be deleted without actually deleting them.
        
        Args:
            table_name: SQL table name
            dataframe: DataFrame containing values
            df_column: Column name in DataFrame
            table_column: Column name in SQL table
            
        Returns:
            DataFrame of rows that would be deleted
        """
        if table_column is None:
            table_column = df_column
            
        try:
            values_to_delete = dataframe[df_column].dropna().unique().tolist()
            
            if not values_to_delete:
                return pd.DataFrame()
            
            if self.db_type == 'sqlite':
                placeholders = ','.join(['?' for _ in values_to_delete])
                query = f"SELECT * FROM {table_name} WHERE {table_column} IN ({placeholders})"
                preview_df = pd.read_sql_query(query, self.connection, params=values_to_delete)
                
            else:
                placeholders = ','.join([f':val_{j}' for j in range(len(values_to_delete))])
                query = f"SELECT * FROM {table_name} WHERE {table_column} IN ({placeholders})"
                params = {f'val_{j}': val for j, val in enumerate(values_to_delete)}
                preview_df = pd.read_sql_query(query, self.engine, params=params)
            
            logger.info(f"🔍 Preview: {len(preview_df)} rows would be deleted")
            return preview_df
            
        except Exception as e:
            logger.error(f"❌ Failed to preview deletion: {e}")
            return pd.DataFrame()
    
    def close(self):
        """Close database connection."""
        try:
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()
            logger.info("🔒 Database connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing connection: {e}")


def create_sample_dataframes():
    """Create sample DataFrames for deletion examples."""
    
    # DataFrame with IDs to delete
    ids_to_delete_df = pd.DataFrame({
        'employee_id': [2, 4, 6, 8],
        'reason': ['Resigned', 'Terminated', 'Retired', 'Transferred']
    })
    
    # DataFrame with multiple criteria
    multi_criteria_df = pd.DataFrame({
        'name': ['Bob Smith', 'Frank Miller'],
        'department': ['Finance', 'Finance'],
        'action': ['Delete', 'Delete']
    })
    
    # DataFrame with departments to clean up
    dept_cleanup_df = pd.DataFrame({
        'department': ['HR', 'Marketing'],
        'reason': ['Department closed', 'Restructuring']
    })
    
    return ids_to_delete_df, multi_criteria_df, dept_cleanup_df


def demonstrate_deletion_methods():
    """Demonstrate various deletion methods."""
    
    print("=" * 80)
    print("🚀 SQL TABLE ROW DELETION FROM DATAFRAME DEMONSTRATION")
    print("=" * 80)
    
    # Initialize deleter with SQLite (easiest for demo)
    deleter = SQLTableDeleter(connection_string='demo_database.db', db_type='sqlite')
    
    if not deleter.connect():
        print("❌ Failed to connect to database")
        return
    
    # Create sample table
    table_name = 'employees'
    deleter.create_sample_table(table_name)
    
    # Show initial data
    print("\n📊 Initial table data:")
    initial_data = deleter.get_table_data(table_name)
    print(initial_data)
    print(f"Initial row count: {len(initial_data)}")
    
    # Create sample DataFrames
    ids_df, multi_df, dept_df = create_sample_dataframes()
    
    # Example 1: Delete by single column (ID)
    print("\n" + "="*60)
    print("1. DELETE BY EMPLOYEE IDs")
    print("="*60)
    print("DataFrame with IDs to delete:")
    print(ids_df)
    
    # Preview deletion
    preview = deleter.safe_delete_preview(table_name, ids_df, 'employee_id', 'id')
    print(f"\nRows that will be deleted:")
    print(preview[['id', 'name', 'department']])
    
    # Perform deletion
    deleted_count = deleter.delete_by_single_column(
        table_name, ids_df, 'employee_id', 'id'
    )
    
    # Show results
    remaining_data = deleter.get_table_data(table_name)
    print(f"\nRemaining rows: {len(remaining_data)}")
    print(remaining_data[['id', 'name', 'department']])
    
    # Example 2: Delete by multiple columns
    print("\n" + "="*60)
    print("2. DELETE BY MULTIPLE COLUMNS")
    print("="*60)
    
    # Reset data for next example
    deleter.create_sample_table(table_name)
    
    print("DataFrame with multiple criteria:")
    print(multi_df)
    
    column_mapping = {
        'name': 'name',
        'department': 'department'
    }
    
    deleted_count = deleter.delete_by_multiple_columns(
        table_name, multi_df, column_mapping
    )
    
    remaining_data = deleter.get_table_data(table_name)
    print(f"\nRemaining rows after multi-column deletion: {len(remaining_data)}")
    print(remaining_data[['id', 'name', 'department']])
    
    # Example 3: Delete with additional conditions
    print("\n" + "="*60)
    print("3. DELETE WITH ADDITIONAL CONDITIONS")
    print("="*60)
    
    # Reset data
    deleter.create_sample_table(table_name)
    
    print("DataFrame with departments to delete:")
    print(dept_df)
    
    # Delete inactive employees in specific departments
    conditions = {
        'id_column': 'department',
        'additional_where': 'active = 0'
    }
    
    deleted_count = deleter.delete_with_conditions(
        table_name, dept_df, conditions
    )
    
    remaining_data = deleter.get_table_data(table_name)
    print(f"\nRemaining rows after conditional deletion: {len(remaining_data)}")
    print(remaining_data[['id', 'name', 'department', 'active']])
    
    # Close connection
    deleter.close()
    
    print("\n" + "="*80)
    print("✅ DEMONSTRATION COMPLETED")
    print("="*80)


def show_connection_examples():
    """Show connection string examples for different databases."""
    
    print("\n" + "="*80)
    print("🔗 DATABASE CONNECTION EXAMPLES")
    print("="*80)
    
    examples = {
        "SQLite": {
            "connection_string": "database_file.db",
            "example": """
# SQLite Example
deleter = SQLTableDeleter('my_database.db', 'sqlite')
deleter.connect()
            """
        },
        
        "SQL Server": {
            "connection_string": "mssql+pymssql://user:password@server/database",
            "example": """
# SQL Server Example
conn_str = "mssql+pymssql://username:password@server:1433/database_name"
deleter = SQLTableDeleter(conn_str, 'mssql')
deleter.connect()
            """
        },
        
        "PostgreSQL": {
            "connection_string": "postgresql://user:password@host:port/database",
            "example": """
# PostgreSQL Example
conn_str = "postgresql://username:password@localhost:5432/database_name"
deleter = SQLTableDeleter(conn_str, 'postgresql')
deleter.connect()
            """
        },
        
        "MySQL": {
            "connection_string": "mysql+mysqlconnector://user:password@host/database",
            "example": """
# MySQL Example
conn_str = "mysql+mysqlconnector://username:password@localhost/database_name"
deleter = SQLTableDeleter(conn_str, 'mysql')
deleter.connect()
            """
        }
    }
    
    for db_type, info in examples.items():
        print(f"\n{db_type}:")
        print(f"Connection String Format: {info['connection_string']}")
        print(info['example'])


def show_best_practices():
    """Show best practices for SQL deletion operations."""
    
    print("\n" + "="*80)
    print("💡 BEST PRACTICES FOR SQL DELETIONS")
    print("="*80)
    
    practices = """
1. ALWAYS BACKUP YOUR DATA BEFORE DELETION
   - Create table backups: CREATE TABLE backup_table AS SELECT * FROM original_table
   - Export important data before bulk deletions

2. USE TRANSACTIONS FOR SAFETY
   - Wrap deletions in BEGIN/COMMIT transactions
   - Use ROLLBACK if something goes wrong

3. TEST WITH PREVIEW FIRST
   - Use safe_delete_preview() to see what will be deleted
   - Verify the results match your expectations

4. BATCH LARGE DELETIONS
   - Process large datasets in smaller batches
   - Avoid memory issues and long-running transactions

5. VALIDATE YOUR DATAFRAME DATA
   - Check for NULL values that might cause issues
   - Ensure data types match between DataFrame and SQL table

6. USE PARAMETERIZED QUERIES
   - Prevents SQL injection attacks
   - This script automatically uses parameterized queries

7. LOG YOUR OPERATIONS
   - Keep track of what was deleted and when
   - Enable logging for audit trails

8. CONSIDER SOFT DELETES
   - Instead of DELETE, use UPDATE to mark records as inactive
   - Allows for data recovery if needed

9. VERIFY DELETIONS
   - Count rows before and after deletion
   - Check that the right records were removed

10. HANDLE FOREIGN KEY CONSTRAINTS
    - Delete child records before parent records
    - Consider CASCADE options carefully
    """
    
    print(practices)


def main():
    """Main function to run demonstrations."""
    
    try:
        # Run main demonstration
        demonstrate_deletion_methods()
        
        # Show connection examples
        show_connection_examples()
        
        # Show best practices
        show_best_practices()
        
        # Cleanup demo database file
        if os.path.exists('demo_database.db'):
            os.remove('demo_database.db')
            print("\n🧹 Cleaned up demo database file")
        
        print("\n🎉 All examples completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error in main demonstration: {e}")


if __name__ == "__main__":
    main()