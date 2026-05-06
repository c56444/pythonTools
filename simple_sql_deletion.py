#!/usr/bin/env python3
"""
Simple SQL Row Deletion from DataFrame

Basic examples of deleting SQL table rows using pandas DataFrame values.
Perfect for common scenarios like bulk deletions and data cleanup.

Required packages:
    pip install pandas sqlalchemy

Usage:
    python simple_sql_deletion.py
"""

import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
import os


def setup_sample_database():
    """Create a sample database and table for examples."""
    
    # Create SQLite database
    conn = sqlite3.connect('sample.db')
    cursor = conn.cursor()
    
    # Create employees table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            department TEXT,
            salary REAL,
            hire_date TEXT,
            active BOOLEAN DEFAULT 1
        )
    ''')
    
    # Insert sample data
    employees = [
        (1, 'John Doe', 'john.doe@company.com', 'IT', 75000, '2023-01-15', 1),
        (2, 'Jane Smith', 'jane.smith@company.com', 'HR', 68000, '2023-02-01', 1),
        (3, 'Mike Johnson', 'mike.johnson@company.com', 'Finance', 72000, '2023-01-20', 1),
        (4, 'Sarah Wilson', 'sarah.wilson@company.com', 'IT', 78000, '2023-03-01', 1),
        (5, 'Tom Brown', 'tom.brown@company.com', 'Marketing', 65000, '2023-02-15', 1),
        (6, 'Lisa Davis', 'lisa.davis@company.com', 'HR', 70000, '2023-01-10', 0),
        (7, 'Chris Miller', 'chris.miller@company.com', 'Finance', 74000, '2023-03-10', 1),
        (8, 'Amy Garcia', 'amy.garcia@company.com', 'IT', 76000, '2023-02-20', 1),
        (9, 'David Lee', 'david.lee@company.com', 'Marketing', 67000, '2023-01-25', 1),
        (10, 'Emma Taylor', 'emma.taylor@company.com', 'Finance', 71000, '2023-03-05', 1)
    ]
    
    cursor.executemany('''
        INSERT OR REPLACE INTO employees 
        (id, name, email, department, salary, hire_date, active) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', employees)
    
    conn.commit()
    conn.close()
    
    print("✅ Sample database created with employee data")


def show_current_data():
    """Display current database contents."""
    conn = sqlite3.connect('sample.db')
    df = pd.read_sql_query("SELECT * FROM employees ORDER BY id", conn)
    conn.close()
    
    print("\n📊 Current Employee Data:")
    print(df.to_string(index=False))
    print(f"Total employees: {len(df)}")
    return df


def example_1_delete_by_ids():
    """Example 1: Delete employees by ID using DataFrame."""
    
    print("\n" + "="*70)
    print("EXAMPLE 1: DELETE BY EMPLOYEE IDs")
    print("="*70)
    
    # Create DataFrame with IDs to delete
    ids_to_delete = pd.DataFrame({
        'employee_id': [2, 4, 6],
        'reason': ['Resigned', 'Terminated', 'Inactive']
    })
    
    print("\nEmployees to delete:")
    print(ids_to_delete)
    
    # Connect to database
    conn = sqlite3.connect('sample.db')
    
    # Get the list of IDs to delete
    id_list = ids_to_delete['employee_id'].tolist()
    
    # Create placeholder string for SQL IN clause
    placeholders = ','.join(['?' for _ in id_list])
    
    # Execute DELETE statement
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", id_list)
    
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"\n✅ Deleted {deleted_count} employees")
    
    # Show remaining data
    show_current_data()


def example_2_delete_by_department():
    """Example 2: Delete all employees from specific departments."""
    
    print("\n" + "="*70)
    print("EXAMPLE 2: DELETE BY DEPARTMENT")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    # Create DataFrame with departments to remove
    dept_to_delete = pd.DataFrame({
        'department': ['HR', 'Marketing'],
        'closure_date': ['2024-12-31', '2024-12-31']
    })
    
    print("\nDepartments to close:")
    print(dept_to_delete)
    
    # Connect and delete
    conn = sqlite3.connect('sample.db')
    
    dept_list = dept_to_delete['department'].tolist()
    placeholders = ','.join(['?' for _ in dept_list])
    
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM employees WHERE department IN ({placeholders})", dept_list)
    
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"\n✅ Deleted {deleted_count} employees from closed departments")
    show_current_data()


def example_3_delete_by_multiple_criteria():
    """Example 3: Delete using multiple columns from DataFrame."""
    
    print("\n" + "="*70)
    print("EXAMPLE 3: DELETE BY MULTIPLE CRITERIA")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    # DataFrame with specific employees to remove
    employees_to_remove = pd.DataFrame({
        'name': ['John Doe', 'Lisa Davis'],
        'department': ['IT', 'HR'],
        'reason': ['Performance', 'Inactive']
    })
    
    print("\nSpecific employees to remove:")
    print(employees_to_remove)
    
    conn = sqlite3.connect('sample.db')
    cursor = conn.cursor()
    
    total_deleted = 0
    
    # Delete each row based on multiple criteria
    for _, row in employees_to_remove.iterrows():
        cursor.execute('''
            DELETE FROM employees 
            WHERE name = ? AND department = ?
        ''', (row['name'], row['department']))
        
        total_deleted += cursor.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Deleted {total_deleted} employees using multiple criteria")
    show_current_data()


def example_4_safe_preview_deletion():
    """Example 4: Preview what will be deleted before actual deletion."""
    
    print("\n" + "="*70)
    print("EXAMPLE 4: SAFE PREVIEW BEFORE DELETION")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    # DataFrame with salary threshold
    salary_criteria = pd.DataFrame({
        'min_salary': [70000],
        'department': ['Finance'],
        'action': ['Remove high earners']
    })
    
    print("\nDeletion criteria:")
    print(salary_criteria)
    
    # Preview what will be deleted
    conn = sqlite3.connect('sample.db')
    
    preview_query = '''
        SELECT * FROM employees 
        WHERE salary >= ? AND department = ?
    '''
    
    preview_df = pd.read_sql_query(
        preview_query, 
        conn, 
        params=(salary_criteria['min_salary'].iloc[0], 
               salary_criteria['department'].iloc[0])
    )
    
    print(f"\n🔍 PREVIEW: {len(preview_df)} employees would be deleted:")
    print(preview_df[['id', 'name', 'department', 'salary']].to_string(index=False))
    
    # Ask for confirmation (in real scenario)
    print(f"\nProceeding with deletion...")
    
    # Execute the deletion
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM employees 
        WHERE salary >= ? AND department = ?
    ''', (salary_criteria['min_salary'].iloc[0], 
          salary_criteria['department'].iloc[0]))
    
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"✅ Deleted {deleted_count} employees")
    show_current_data()


def example_5_batch_deletion():
    """Example 5: Batch deletion for large datasets."""
    
    print("\n" + "="*70)
    print("EXAMPLE 5: BATCH DELETION")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    # Create a larger DataFrame for batch processing
    large_id_list = pd.DataFrame({
        'employee_id': [1, 3, 5, 7, 9],
        'batch': ['A', 'A', 'B', 'B', 'C']
    })
    
    print("\nLarge list of IDs to delete:")
    print(large_id_list)
    
    conn = sqlite3.connect('sample.db')
    cursor = conn.cursor()
    
    batch_size = 2  # Small batch for demonstration
    total_deleted = 0
    
    # Process in batches
    id_list = large_id_list['employee_id'].tolist()
    
    for i in range(0, len(id_list), batch_size):
        batch = id_list[i:i + batch_size]
        placeholders = ','.join(['?' for _ in batch])
        
        cursor.execute(f"DELETE FROM employees WHERE id IN ({placeholders})", batch)
        batch_deleted = cursor.rowcount
        total_deleted += batch_deleted
        
        print(f"Batch {i//batch_size + 1}: Deleted {batch_deleted} rows")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Total deleted in batches: {total_deleted} employees")
    show_current_data()


def example_6_sqlalchemy_approach():
    """Example 6: Using SQLAlchemy instead of raw sqlite3."""
    
    print("\n" + "="*70)
    print("EXAMPLE 6: SQLALCHEMY APPROACH")
    print("="*70)
    
    # Reset database
    setup_sample_database()
    
    # Create SQLAlchemy engine
    engine = create_engine('sqlite:///sample.db')
    
    # DataFrame with emails to delete
    emails_to_delete = pd.DataFrame({
        'email': ['john.doe@company.com', 'emma.taylor@company.com'],
        'reason': ['Duplicate account', 'Left company']
    })
    
    print("\nEmployees to delete by email:")
    print(emails_to_delete)
    
    # Get email list
    email_list = emails_to_delete['email'].tolist()
    
    # Create parameterized query
    placeholders = ','.join([f':email_{i}' for i in range(len(email_list))])
    query = f"DELETE FROM employees WHERE email IN ({placeholders})"
    
    # Create parameters dictionary
    params = {f'email_{i}': email for i, email in enumerate(email_list)}
    
    # Execute with SQLAlchemy
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        deleted_count = result.rowcount
        conn.commit()
    
    print(f"\n✅ Deleted {deleted_count} employees using SQLAlchemy")
    show_current_data()
    
    engine.dispose()


def show_code_templates():
    """Show reusable code templates."""
    
    print("\n" + "="*70)
    print("📋 REUSABLE CODE TEMPLATES")
    print("="*70)
    
    templates = """
# Template 1: Delete by single column
def delete_by_column(df, column_name, table_name, db_connection):
    values = df[column_name].dropna().unique().tolist()
    placeholders = ','.join(['?' for _ in values])
    query = f"DELETE FROM {table_name} WHERE {column_name} IN ({placeholders})"
    
    cursor = db_connection.cursor()
    cursor.execute(query, values)
    deleted_count = cursor.rowcount
    db_connection.commit()
    return deleted_count

# Template 2: Delete by multiple criteria  
def delete_by_criteria(df, criteria_dict, table_name, db_connection):
    cursor = db_connection.cursor()
    total_deleted = 0
    
    for _, row in df.iterrows():
        conditions = []
        params = []
        
        for df_col, table_col in criteria_dict.items():
            conditions.append(f"{table_col} = ?")
            params.append(row[df_col])
        
        where_clause = " AND ".join(conditions)
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        
        cursor.execute(query, params)
        total_deleted += cursor.rowcount
    
    db_connection.commit()
    return total_deleted

# Template 3: Safe preview before deletion
def preview_deletion(df, column_name, table_name, db_connection):
    values = df[column_name].dropna().unique().tolist()
    placeholders = ','.join(['?' for _ in values])
    query = f"SELECT * FROM {table_name} WHERE {column_name} IN ({placeholders})"
    
    preview_df = pd.read_sql_query(query, db_connection, params=values)
    print(f"Preview: {len(preview_df)} rows would be deleted")
    return preview_df
    """
    
    print(templates)


def cleanup():
    """Remove sample database file."""
    if os.path.exists('sample.db'):
        os.remove('sample.db')
        print("\n🧹 Cleaned up sample database file")


def main():
    """Run all examples."""
    
    print("SQL TABLE ROW DELETION FROM DATAFRAME - SIMPLE EXAMPLES")
    print("="*70)
    
    try:
        # Set up sample database
        setup_sample_database()
        show_current_data()
        
        # Run examples
        example_1_delete_by_ids()
        example_2_delete_by_department() 
        example_3_delete_by_multiple_criteria()
        example_4_safe_preview_deletion()
        example_5_batch_deletion()
        example_6_sqlalchemy_approach()
        
        # Show templates
        show_code_templates()
        
        print("\n" + "="*70)
        print("✅ ALL EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*70)
        
        print("\nKey takeaways:")
        print("• Use parameterized queries to prevent SQL injection")
        print("• Always preview deletions before executing them") 
        print("• Process large datasets in batches")
        print("• Use transactions to ensure data consistency")
        print("• Keep backups before bulk deletions")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        cleanup()


if __name__ == "__main__":
    main()