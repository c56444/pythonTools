#!/usr/bin/env python3
"""
Simple DataFrame Row Deletion Examples

Common patterns for deleting rows from pandas DataFrames.

Required packages:
    pip install pandas numpy
"""

import pandas as pd
import numpy as np

def create_sample_dataframe():
    """Create a sample DataFrame for examples."""
    data = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 
                'Frank', 'Grace', 'Henry', 'Iris', 'Jack'],
        'age': [25, 30, 35, 28, 32, 45, 29, 38, 27, 33],
        'salary': [50000, 60000, 70000, 55000, 65000, 
                  80000, 58000, 75000, 52000, 68000],
        'department': ['IT', 'HR', 'Finance', 'IT', 'Marketing',
                      'Finance', 'HR', 'IT', 'Marketing', 'Finance']
    }
    return pd.DataFrame(data)

def example_delete_by_index():
    """Delete rows by index position."""
    print("=" * 60)
    print("1. DELETE BY INDEX POSITION")
    print("=" * 60)
    
    df = create_sample_dataframe()
    print("Original DataFrame:")
    print(df)
    print(f"\nShape: {df.shape}")
    
    # Delete single row by index
    df_single = df.drop(0).reset_index(drop=True)
    print(f"\nAfter deleting row 0:")
    print(df_single.head())
    
    # Delete multiple rows by index
    df_multiple = df.drop([0, 2, 4]).reset_index(drop=True)
    print(f"\nAfter deleting rows 0, 2, 4:")
    print(df_multiple.head())
    
    # Delete by index range
    df_range = df.drop(df.index[2:5]).reset_index(drop=True)
    print(f"\nAfter deleting index range 2:5:")
    print(df_range.head())

def example_delete_by_condition():
    """Delete rows based on conditions."""
    print("\n" + "=" * 60)
    print("2. DELETE BY CONDITION")
    print("=" * 60)
    
    df = create_sample_dataframe()
    
    # Delete rows where age > 35
    print("Original DataFrame:")
    print(df[['name', 'age', 'salary']])
    
    df_age = df[df['age'] <= 35].reset_index(drop=True)
    print(f"\nAfter deleting rows where age > 35:")
    print(df_age[['name', 'age', 'salary']])
    
    # Delete rows where salary < 60000
    df_salary = df[df['salary'] >= 60000].reset_index(drop=True)
    print(f"\nAfter deleting rows where salary < 60000:")
    print(df_salary[['name', 'age', 'salary']])
    
    # Delete rows with multiple conditions
    df_multi = df[(df['age'] <= 35) & (df['salary'] >= 55000)].reset_index(drop=True)
    print(f"\nAfter deleting rows where age > 35 OR salary < 55000:")
    print(df_multi[['name', 'age', 'salary']])

def example_delete_by_value():
    """Delete rows containing specific values."""
    print("\n" + "=" * 60)
    print("3. DELETE BY SPECIFIC VALUES")
    print("=" * 60)
    
    df = create_sample_dataframe()
    print("Original DataFrame:")
    print(df[['name', 'department']])
    
    # Delete rows where department is 'IT'
    df_no_it = df[df['department'] != 'IT'].reset_index(drop=True)
    print(f"\nAfter deleting IT department:")
    print(df_no_it[['name', 'department']])
    
    # Delete rows where department is in list
    departments_to_remove = ['IT', 'HR']
    df_filtered = df[~df['department'].isin(departments_to_remove)].reset_index(drop=True)
    print(f"\nAfter deleting IT and HR departments:")
    print(df_filtered[['name', 'department']])
    
    # Delete rows where name contains specific pattern
    df_no_a = df[~df['name'].str.contains('a', case=False)].reset_index(drop=True)
    print(f"\nAfter deleting names containing 'a':")
    print(df_no_a[['name', 'department']])

def example_delete_duplicates():
    """Delete duplicate rows."""
    print("\n" + "=" * 60)
    print("4. DELETE DUPLICATE ROWS")
    print("=" * 60)
    
    # Create DataFrame with duplicates
    df = create_sample_dataframe()
    # Add duplicate rows
    df.loc[len(df)] = df.iloc[0]  # Duplicate first row
    df.loc[len(df)] = df.iloc[1]  # Duplicate second row
    
    print("DataFrame with duplicates:")
    print(df.tail(7))
    print(f"Shape: {df.shape}")
    
    # Remove exact duplicates
    df_no_dups = df.drop_duplicates().reset_index(drop=True)
    print(f"\nAfter removing exact duplicates:")
    print(df_no_dups.tail(5))
    print(f"Shape: {df_no_dups.shape}")
    
    # Remove duplicates based on specific columns
    df_no_name_dups = df.drop_duplicates(subset=['name']).reset_index(drop=True)
    print(f"\nAfter removing duplicates based on 'name' column:")
    print(df_no_name_dups.tail(5))
    print(f"Shape: {df_no_name_dups.shape}")

def example_delete_missing_values():
    """Delete rows with missing values."""
    print("\n" + "=" * 60)
    print("5. DELETE ROWS WITH MISSING VALUES")
    print("=" * 60)
    
    df = create_sample_dataframe()
    # Add some missing values
    df.loc[2, 'salary'] = np.nan
    df.loc[5, 'age'] = np.nan
    df.loc[8, 'name'] = np.nan
    
    print("DataFrame with missing values:")
    print(df)
    print(f"Shape: {df.shape}")
    
    # Show missing value counts
    print(f"\nMissing values per column:")
    print(df.isnull().sum())
    
    # Delete rows with any missing values
    df_no_na = df.dropna().reset_index(drop=True)
    print(f"\nAfter deleting rows with any missing values:")
    print(df_no_na)
    print(f"Shape: {df_no_na.shape}")
    
    # Delete rows with missing values in specific columns
    df_reset = create_sample_dataframe()
    df_reset.loc[2, 'salary'] = np.nan
    df_reset.loc[5, 'age'] = np.nan
    
    df_salary_only = df_reset.dropna(subset=['salary']).reset_index(drop=True)
    print(f"\nAfter deleting rows with missing salary only:")
    print(df_salary_only[['name', 'age', 'salary']])

def example_practical_scenarios():
    """Show practical deletion scenarios."""
    print("\n" + "=" * 60)
    print("6. PRACTICAL DELETION SCENARIOS")
    print("=" * 60)
    
    df = create_sample_dataframe()
    
    # Scenario 1: Remove outliers (salary > 2 standard deviations)
    mean_salary = df['salary'].mean()
    std_salary = df['salary'].std()
    threshold = mean_salary + 2 * std_salary
    
    print(f"Removing salary outliers > {threshold:.0f}")
    df_no_outliers = df[df['salary'] <= threshold].reset_index(drop=True)
    print(df_no_outliers[['name', 'salary']])
    
    # Scenario 2: Remove bottom 20% by salary
    print(f"\nRemoving bottom 20% by salary:")
    bottom_20_threshold = df['salary'].quantile(0.2)
    df_top_80 = df[df['salary'] > bottom_20_threshold].reset_index(drop=True)
    print(df_top_80[['name', 'salary']])
    
    # Scenario 3: Keep only specific departments
    print(f"\nKeeping only IT and Finance departments:")
    df_filtered_dept = df[df['department'].isin(['IT', 'Finance'])].reset_index(drop=True)
    print(df_filtered_dept[['name', 'department']])

def show_deletion_summary():
    """Show summary of deletion methods."""
    print("\n" + "=" * 60)
    print("DELETION METHODS SUMMARY")
    print("=" * 60)
    
    summary = """
    METHOD                           SYNTAX                                    USE CASE
    ─────────────────────────────────────────────────────────────────────────────────────
    Delete by index                  df.drop([0, 1, 2])                      Remove specific rows
    Delete by condition              df[df['column'] > value]                 Remove based on criteria  
    Delete by value                  df[df['column'] != value]                Remove specific values
    Delete multiple values           df[~df['column'].isin([val1, val2])]    Remove multiple values
    Delete duplicates               df.drop_duplicates()                      Remove duplicate rows
    Delete missing values           df.dropna()                               Remove rows with NaN
    Delete with multiple conditions df[(df['col1'] > x) & (df['col2'] < y)]   Complex filtering
    Delete by string pattern        df[~df['col'].str.contains('pattern')]   Remove text patterns
    
    IMPORTANT NOTES:
    • Always use .reset_index(drop=True) after deletion
    • Make a backup: df_backup = df.copy() 
    • Use ~ operator for NOT condition: df[~condition]
    • Use & for AND, | for OR in conditions
    • Wrap conditions in parentheses: (condition1) & (condition2)
    """
    print(summary)

def main():
    """Run all examples."""
    print("PANDAS DATAFRAME ROW DELETION EXAMPLES")
    print("=" * 60)
    
    example_delete_by_index()
    example_delete_by_condition()
    example_delete_by_value()
    example_delete_duplicates()
    example_delete_missing_values()
    example_practical_scenarios()
    show_deletion_summary()
    
    print("\n✅ All examples completed!")

if __name__ == "__main__":
    main()