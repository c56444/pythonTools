#!/usr/bin/env python3
"""
Pandas DataFrame Row Deletion Script

This script demonstrates various methods to delete rows from a pandas DataFrame:
- Delete by index position
- Delete by condition
- Delete by value
- Delete duplicates
- Delete rows with missing values
- Delete rows based on multiple conditions

Required packages:
    pip install pandas numpy

Usage:
    python delete_dataframe_rows.py
"""

import pandas as pd
import numpy as np
from typing import List, Union, Optional
import warnings

# Suppress pandas future warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)


class DataFrameRowDeleter:
    """Utility class for demonstrating various row deletion methods in pandas."""
    
    def __init__(self):
        self.df = None
        self.original_df = None
    
    def create_sample_data(self) -> pd.DataFrame:
        """Create sample data for demonstration."""
        np.random.seed(42)  # For reproducible results
        
        data = {
            'id': range(1, 21),
            'name': [
                'Alice', 'Bob', 'Charlie', 'Diana', 'Eve',
                'Frank', 'Grace', 'Henry', 'Iris', 'Jack',
                'Kate', 'Liam', 'Mia', 'Noah', 'Olivia',
                'Paul', 'Quinn', 'Rachel', 'Sam', 'Tina'
            ],
            'age': [25, 30, 35, 28, 32, 45, 29, 38, 27, 33,
                   41, 26, 31, 36, 24, 39, 28, 34, 30, 37],
            'salary': [50000, 60000, 70000, 55000, 65000, 80000, 58000, 75000, 52000, 68000,
                      78000, 51000, 62000, 72000, 48000, 76000, 54000, 69000, 61000, 74000],
            'department': [
                'IT', 'HR', 'Finance', 'IT', 'Marketing',
                'Finance', 'HR', 'IT', 'Marketing', 'Finance',
                'IT', 'HR', 'Marketing', 'Finance', 'IT',
                'HR', 'Marketing', 'Finance', 'IT', 'HR'
            ],
            'active': [True, True, False, True, True, False, True, True, False, True,
                      True, False, True, True, False, True, True, False, True, True]
        }
        
        # Add some duplicate rows and missing values for demonstration
        df = pd.DataFrame(data)
        
        # Add duplicate rows
        duplicate_row = df.iloc[0].copy()
        df.loc[len(df)] = duplicate_row
        df.loc[len(df)] = duplicate_row
        
        # Add some missing values
        df.loc[5, 'salary'] = np.nan
        df.loc[8, 'age'] = np.nan
        df.loc[12, 'name'] = np.nan
        
        self.df = df.copy()
        self.original_df = df.copy()
        
        print("📊 Sample DataFrame created:")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        print("\nFirst 5 rows:")
        print(df.head())
        print("\nLast 5 rows:")
        print(df.tail())
        
        return df
    
    def reset_dataframe(self):
        """Reset DataFrame to original state."""
        self.df = self.original_df.copy()
        print("\n🔄 DataFrame reset to original state")
    
    def delete_by_index(self, indices: Union[int, List[int]]) -> pd.DataFrame:
        """Delete rows by index position(s)."""
        print(f"\n🗑️  Deleting rows by index: {indices}")
        print(f"   Original shape: {self.df.shape}")
        
        # Method 1: Using drop() method (recommended)
        df_result = self.df.drop(indices).reset_index(drop=True)
        
        print(f"   New shape: {df_result.shape}")
        print(f"   Deleted {self.df.shape[0] - df_result.shape[0]} row(s)")
        
        return df_result
    
    def delete_by_condition(self, condition_description: str, condition) -> pd.DataFrame:
        """Delete rows based on a condition."""
        print(f"\n🔍 Deleting rows where: {condition_description}")
        print(f"   Original shape: {self.df.shape}")
        
        # Count rows that match the condition
        rows_to_delete = self.df[condition]
        print(f"   Rows matching condition: {len(rows_to_delete)}")
        
        # Delete rows that DON'T match the condition (keep rows that don't match)
        df_result = self.df[~condition].reset_index(drop=True)
        
        print(f"   New shape: {df_result.shape}")
        print(f"   Deleted {self.df.shape[0] - df_result.shape[0]} row(s)")
        
        if len(rows_to_delete) > 0:
            print("   Sample deleted rows:")
            print(rows_to_delete.head(3).to_string())
        
        return df_result
    
    def delete_by_value(self, column: str, values: Union[str, int, List]) -> pd.DataFrame:
        """Delete rows where a column contains specific value(s)."""
        if not isinstance(values, list):
            values = [values]
        
        print(f"\n🎯 Deleting rows where '{column}' is in {values}")
        print(f"   Original shape: {self.df.shape}")
        
        # Create condition for rows to delete
        condition = self.df[column].isin(values)
        rows_to_delete = self.df[condition]
        
        print(f"   Rows to delete: {len(rows_to_delete)}")
        
        # Keep rows that don't match the condition
        df_result = self.df[~condition].reset_index(drop=True)
        
        print(f"   New shape: {df_result.shape}")
        print(f"   Deleted {self.df.shape[0] - df_result.shape[0]} row(s)")
        
        return df_result
    
    def delete_duplicates(self, subset: Optional[List[str]] = None, keep: str = 'first') -> pd.DataFrame:
        """Delete duplicate rows."""
        print(f"\n🔄 Deleting duplicate rows")
        if subset:
            print(f"   Based on columns: {subset}")
        else:
            print("   Based on all columns")
        print(f"   Keep strategy: {keep}")
        print(f"   Original shape: {self.df.shape}")
        
        df_result = self.df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)
        
        print(f"   New shape: {df_result.shape}")
        print(f"   Deleted {self.df.shape[0] - df_result.shape[0]} duplicate row(s)")
        
        return df_result
    
    def delete_rows_with_missing_values(self, how: str = 'any', subset: Optional[List[str]] = None) -> pd.DataFrame:
        """Delete rows with missing values (NaN)."""
        print(f"\n❓ Deleting rows with missing values")
        if subset:
            print(f"   Checking columns: {subset}")
        else:
            print("   Checking all columns")
        print(f"   Strategy: {how} (any/all)")
        print(f"   Original shape: {self.df.shape}")
        
        # Show missing values before deletion
        missing_counts = self.df.isnull().sum()
        print("   Missing values per column:")
        for col, count in missing_counts.items():
            if count > 0:
                print(f"     {col}: {count}")
        
        df_result = self.df.dropna(how=how, subset=subset).reset_index(drop=True)
        
        print(f"   New shape: {df_result.shape}")
        print(f"   Deleted {self.df.shape[0] - df_result.shape[0]} row(s)")
        
        return df_result
    
    def delete_by_multiple_conditions(self) -> pd.DataFrame:
        """Delete rows based on multiple conditions (AND/OR logic)."""
        print(f"\n🔗 Deleting rows with multiple conditions")
        print("   Condition: (age > 35) AND (salary < 60000)")
        print(f"   Original shape: {self.df.shape}")
        
        # Multiple conditions with AND
        condition = (self.df['age'] > 35) & (self.df['salary'] < 60000)
        rows_to_delete = self.df[condition]
        
        print(f"   Rows matching conditions: {len(rows_to_delete)}")
        
        df_result = self.df[~condition].reset_index(drop=True)
        
        print(f"   New shape: {df_result.shape}")
        print(f"   Deleted {self.df.shape[0] - df_result.shape[0]} row(s)")
        
        if len(rows_to_delete) > 0:
            print("   Deleted rows:")
            print(rows_to_delete[['name', 'age', 'salary']].to_string())
        
        return df_result
    
    def delete_by_query_string(self, query: str) -> pd.DataFrame:
        """Delete rows using pandas query string."""
        print(f"\n🔍 Deleting rows using query: '{query}'")
        print(f"   Original shape: {self.df.shape}")
        
        # Find rows that match the query (to be deleted)
        try:
            rows_to_delete = self.df.query(query)
            print(f"   Rows matching query: {len(rows_to_delete)}")
            
            # Keep rows that don't match the query
            df_result = self.df.query(f"not ({query})").reset_index(drop=True)
            
            print(f"   New shape: {df_result.shape}")
            print(f"   Deleted {self.df.shape[0] - df_result.shape[0]} row(s)")
            
            return df_result
        
        except Exception as e:
            print(f"   ❌ Error in query: {e}")
            return self.df.copy()
    
    def demonstrate_all_methods(self):
        """Demonstrate all deletion methods."""
        print("=" * 80)
        print("🚀 PANDAS DATAFRAME ROW DELETION DEMONSTRATION")
        print("=" * 80)
        
        # Create sample data
        self.create_sample_data()
        
        # 1. Delete by index
        self.reset_dataframe()
        result1 = self.delete_by_index([0, 2, 4])
        
        # 2. Delete by condition - age > 35
        self.reset_dataframe()
        result2 = self.delete_by_condition("age > 35", self.df['age'] > 35)
        
        # 3. Delete by value - department
        self.reset_dataframe()
        result3 = self.delete_by_value('department', ['IT', 'HR'])
        
        # 4. Delete by boolean condition - inactive employees
        self.reset_dataframe()
        result4 = self.delete_by_condition("active == False", self.df['active'] == False)
        
        # 5. Delete duplicates
        self.reset_dataframe()
        result5 = self.delete_duplicates()
        
        # 6. Delete rows with missing values
        self.reset_dataframe()
        result6 = self.delete_rows_with_missing_values()
        
        # 7. Delete by multiple conditions
        self.reset_dataframe()
        result7 = self.delete_by_multiple_conditions()
        
        # 8. Delete using query string
        self.reset_dataframe()
        result8 = self.delete_by_query_string("department == 'Finance' and salary > 70000")
        
        print("\n" + "=" * 80)
        print("✅ DEMONSTRATION COMPLETED")
        print("=" * 80)


def demonstrate_advanced_techniques():
    """Demonstrate advanced deletion techniques."""
    print("\n" + "=" * 80)
    print("🔬 ADVANCED DELETION TECHNIQUES")
    print("=" * 80)
    
    # Create sample data
    np.random.seed(42)
    df = pd.DataFrame({
        'A': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'B': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'],
        'C': [10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7, 80.8, 90.9, 100.0],
        'D': [True, False, True, False, True, False, True, False, True, False]
    })
    
    print("Original DataFrame:")
    print(df)
    print(f"Shape: {df.shape}\n")
    
    # 1. Delete rows by index range
    print("1. Delete rows by index range (2:5):")
    result1 = df.drop(df.index[2:5])
    print(result1)
    print(f"Shape: {result1.shape}\n")
    
    # 2. Delete rows by percentile
    print("2. Delete rows with values in top 20% of column C:")
    threshold = df['C'].quantile(0.8)
    result2 = df[df['C'] <= threshold]
    print(f"Threshold: {threshold}")
    print(result2)
    print(f"Shape: {result2.shape}\n")
    
    # 3. Delete rows using sample()
    print("3. Randomly delete 30% of rows:")
    result3 = df.sample(frac=0.7, random_state=42)  # Keep 70%
    print(result3)
    print(f"Shape: {result3.shape}\n")
    
    # 4. Delete rows using nlargest/nsmallest
    print("4. Delete 3 largest values in column A (keep the rest):")
    largest_indices = df.nlargest(3, 'A').index
    result4 = df.drop(largest_indices)
    print(result4)
    print(f"Shape: {result4.shape}\n")


def show_best_practices():
    """Show best practices for row deletion."""
    print("\n" + "=" * 80)
    print("💡 BEST PRACTICES FOR ROW DELETION")
    print("=" * 80)
    
    practices = [
        "1. Always make a copy of your DataFrame before deletion:",
        "   df_backup = df.copy()",
        "",
        "2. Use reset_index(drop=True) to reset index after deletion:",
        "   df = df.drop(indices).reset_index(drop=True)",
        "",
        "3. For large datasets, use boolean indexing instead of drop():",
        "   # Faster: df = df[df['column'] != value]",
        "   # Slower: df = df.drop(df[df['column'] == value].index)",
        "",
        "4. Chain conditions with parentheses for clarity:",
        "   df = df[(df['age'] > 25) & (df['salary'] < 100000)]",
        "",
        "5. Use inplace=True carefully (modifies original DataFrame):",
        "   df.drop(indices, inplace=True)  # No copy made",
        "",
        "6. Handle missing values before other operations:",
        "   df = df.dropna()  # Remove NaN rows first",
        "",
        "7. Use query() for complex string conditions:",
        "   df = df.query('age > 25 and department == \"IT\"')",
        "",
        "8. Consider memory usage with large datasets:",
        "   # Process in chunks for very large datasets",
        "",
        "9. Validate your deletion logic:",
        "   print(f'Deleted {original_len - new_len} rows')",
        "",
        "10. Use loc[] for label-based deletion, iloc[] for position-based:"
    ]
    
    for practice in practices:
        print(practice)


def main():
    """Main function to run all demonstrations."""
    # Initialize the deleter
    deleter = DataFrameRowDeleter()
    
    # Run main demonstration
    deleter.demonstrate_all_methods()
    
    # Show advanced techniques
    demonstrate_advanced_techniques()
    
    # Show best practices
    show_best_practices()
    
    print("\n" + "=" * 80)
    print("🎉 ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 80)
    print("\nKey takeaways:")
    print("• Use df.drop() for index-based deletion")
    print("• Use boolean indexing df[condition] for condition-based deletion")
    print("• Use df.dropna() for missing value deletion")
    print("• Use df.drop_duplicates() for duplicate deletion")
    print("• Always reset_index(drop=True) after deletion")
    print("• Make backups before deletion operations")


if __name__ == "__main__":
    main()