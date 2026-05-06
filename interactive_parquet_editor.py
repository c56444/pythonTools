#!/usr/bin/env python3
"""
Interactive script to remove entries from parquet file based on FileName.
This version provides a user-friendly interface without requiring command line arguments.
"""

import pandas as pd
import os

def interactive_parquet_editor():
    """Interactive version for removing entries from parquet file."""
    
    parquet_file_path = r"c:\temp\draw_supplement_reportFilePaths.parquet"
    
    print("=" * 60)
    print("   Interactive Parquet File Entry Remover")
    print("=" * 60)
    print(f"Target file: {parquet_file_path}")
    
    # Check if file exists
    if not os.path.exists(parquet_file_path):
        print(f"❌ Error: Parquet file not found at '{parquet_file_path}'")
        input("Press Enter to exit...")
        return
    
    try:
        # Read and display file info
        df = pd.read_parquet(parquet_file_path)
        print(f"\n📊 File Info:")
        print(f"   - Total rows: {len(df)}")
        print(f"   - Columns: {list(df.columns)}")
        
        if 'FileName' not in df.columns:
            print("❌ Error: 'FileName' column not found!")
            input("Press Enter to exit...")
            return
        
        while True:
            print("\n" + "="*60)
            print("What would you like to do?")
            print("1. List all FileNames in the parquet file")
            print("2. Remove a specific FileName")
            print("3. Search for FileNames (partial match)")
            print("4. Exit")
            
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == '1':
                # List all filenames
                unique_filenames = df['FileName'].unique()
                print(f"\n📝 All FileNames ({len(unique_filenames)} unique entries):")
                print("-" * 50)
                for i, filename in enumerate(sorted(unique_filenames), 1):
                    print(f"{i:3d}. {filename}")
                
            elif choice == '2':
                # Remove a specific filename
                filename_to_remove = input("\nEnter the exact FileName to remove: ").strip()
                
                if not filename_to_remove:
                    print("❌ Please enter a valid filename.")
                    continue
                
                # Check if filename exists
                matching_rows = df[df['FileName'] == filename_to_remove]
                if matching_rows.empty:
                    print(f"❌ No entries found with FileName: '{filename_to_remove}'")
                    continue
                
                # Show matching entries
                print(f"\n🎯 Found {len(matching_rows)} matching entries:")
                print("-" * 50)
                print(matching_rows.to_string())
                
                # Confirm removal
                confirm = input(f"\n❓ Remove {len(matching_rows)} entries? (y/N): ").lower().strip()
                if confirm in ['y', 'yes']:
                    # Create backup
                    backup_path = parquet_file_path + '.backup'
                    print(f"💾 Creating backup at: {backup_path}")
                    df.to_parquet(backup_path, index=False)
                    
                    # Remove entries
                    df = df[df['FileName'] != filename_to_remove]
                    
                    # Save updated file
                    df.to_parquet(parquet_file_path, index=False)
                    
                    print(f"✅ Successfully removed entries!")
                    print(f"   New total rows: {len(df)}")
                else:
                    print("❌ Removal cancelled.")
                
            elif choice == '3':
                # Search for filenames
                search_term = input("\nEnter search term (partial filename): ").strip()
                
                if not search_term:
                    print("❌ Please enter a search term.")
                    continue
                
                matching_filenames = df[df['FileName'].str.contains(search_term, case=False, na=False)]['FileName'].unique()
                
                if len(matching_filenames) == 0:
                    print(f"❌ No FileNames found containing: '{search_term}'")
                else:
                    print(f"\n🔍 FileNames containing '{search_term}' ({len(matching_filenames)} found):")
                    print("-" * 50)
                    for i, filename in enumerate(sorted(matching_filenames), 1):
                        count = len(df[df['FileName'] == filename])
                        print(f"{i:3d}. {filename} ({count} entries)")
                
            elif choice == '4':
                print("\n👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        input("Press Enter to exit...")

if __name__ == "__main__":
    try:
        interactive_parquet_editor()
    except KeyboardInterrupt:
        print("\n\n👋 Exiting...")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        input("Press Enter to exit...")