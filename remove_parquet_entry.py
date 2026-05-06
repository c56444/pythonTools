#!/usr/bin/env python3
"""
Script to remove entries from a parquet file based on FileName column.
Usage: python remove_parquet_entry.py <filename_to_remove>
"""

import sys
import pandas as pd
import os
from pathlib import Path

def remove_entry_from_parquet(parquet_file_path, filename_to_remove):
    """
    Remove an entry from parquet file based on FileName column.
    
    Args:
        parquet_file_path (str): Path to the parquet file
        filename_to_remove (str): The FileName value to remove
    
    Returns:
        bool: True if entry was found and removed, False otherwise
    """
    
    # Check if parquet file exists
    if not os.path.exists(parquet_file_path):
        print(f"Error: Parquet file '{parquet_file_path}' not found.")
        return False
    
    try:
        # Read the parquet file
        print(f"Reading parquet file: {parquet_file_path}")
        df = pd.read_parquet(parquet_file_path)
        
        # Display basic info about the file
        print(f"Original data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Check if FileName column exists
        if 'FileName' not in df.columns:
            print("Error: 'FileName' column not found in the parquet file.")
            print(f"Available columns: {list(df.columns)}")
            return False
        
        # Check if the filename exists in the data
        matching_rows = df[df['FileName'] == filename_to_remove]
        if matching_rows.empty:
            print(f"No entries found with FileName: '{filename_to_remove}'")
            return False
        
        # Show matching entries before removal
        print(f"\nFound {len(matching_rows)} matching entries:")
        print(matching_rows.to_string())
        
        # Remove the entries
        df_filtered = df[df['FileName'] != filename_to_remove]
        
        print(f"\nAfter removal data shape: {df_filtered.shape}")
        print(f"Removed {len(df) - len(df_filtered)} entries")
        
        # Create backup of original file
        backup_path = parquet_file_path + '.backup'
        print(f"Creating backup at: {backup_path}")
        df.to_parquet(backup_path, index=False)
        
        # Save the updated data back to the original file
        print(f"Updating parquet file: {parquet_file_path}")
        df_filtered.to_parquet(parquet_file_path, index=False)
        
        print("✅ Successfully removed entries and updated parquet file")
        return True
        
    except Exception as e:
        print(f"Error processing parquet file: {str(e)}")
        return False

def list_filenames_in_parquet(parquet_file_path):
    """
    List all unique FileNames in the parquet file for reference.
    
    Args:
        parquet_file_path (str): Path to the parquet file
    """
    try:
        df = pd.read_parquet(parquet_file_path)
        if 'FileName' in df.columns:
            unique_filenames = df['FileName'].unique()
            print(f"\nUnique FileNames in the parquet file ({len(unique_filenames)} total):")
            for filename in sorted(unique_filenames):
                print(f"  - {filename}")
        else:
            print("Error: 'FileName' column not found in the parquet file.")
    except Exception as e:
        print(f"Error reading parquet file: {str(e)}")

def main():
    """Main function to handle command line arguments and execute the removal."""
    
    parquet_file_path = r"c:\temp\draw_supplement_reportFilePaths.parquet"
    
    if len(sys.argv) < 2:
        print("Usage: python remove_parquet_entry.py <filename_to_remove>")
        print("       python remove_parquet_entry.py --list  (to see all filenames)")
        print(f"\nTarget parquet file: {parquet_file_path}")
        return
    
    # Handle --list option to show all filenames
    if sys.argv[1] == '--list':
        list_filenames_in_parquet(parquet_file_path)
        return
    
    filename_to_remove = sys.argv[1]
    
    print(f"Target parquet file: {parquet_file_path}")
    print(f"FileName to remove: '{filename_to_remove}'")
    print("-" * 50)
    
    # Confirm before proceeding
    confirm = input("Do you want to proceed? (y/N): ").lower().strip()
    if confirm not in ['y', 'yes']:
        print("Operation cancelled.")
        return
    
    # Execute the removal
    success = remove_entry_from_parquet(parquet_file_path, filename_to_remove)
    
    if success:
        print("\n✅ Operation completed successfully!")
    else:
        print("\n❌ Operation failed!")

if __name__ == "__main__":
    main()