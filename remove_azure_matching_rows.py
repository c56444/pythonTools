#!/usr/bin/env python3
"""
Azure Storage Parquet File Cleanup Script

This script connects to an Azure Storage account, lists all files, and removes rows from 
a local parquet file where the FileName column matches files found in the storage account.

The script will:
1. Connect to Azure Storage using various authentication methods
2. List all files across all containers in the storage account
3. Load the parquet file from c:\\temp\\draw_supplement_reportFilePaths.parquet
4. Remove rows where FileName matches any file in Azure Storage
5. Create a backup of the original file before making changes
6. Save the updated parquet file

Required packages:
    pip install azure-storage-blob azure-identity pandas pyarrow

Usage examples:
    # Using connection string
    python remove_azure_matching_rows.py --connection-string "DefaultEndpointsProtocol=https;..."
    
    # Using account name and key
    python remove_azure_matching_rows.py --account-name mystorageaccount --account-key mykey123
    
    # Using managed identity
    python remove_azure_matching_rows.py --account-url https://mystorageaccount.blob.core.windows.net --use-managed-identity
    
    # Using custom parquet file path
    python remove_azure_matching_rows.py --connection-string "..." --parquet-file "c:\\custom\\path\\file.parquet"
    
    # Target specific containers only
    python remove_azure_matching_rows.py --connection-string "..." --containers "manuals,edg-manuals" --dry-run
"""

import os
import argparse
import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict, Any, Set
from pathlib import Path

try:
    from azure.storage.blob import BlobServiceClient, ContainerClient
    from azure.identity import DefaultAzureCredential
    from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError
except ImportError as e:
    print(f"Error importing Azure libraries: {e}")
    print("Please install required packages: pip install azure-storage-blob azure-identity pandas pyarrow")
    exit(1)


def prompt_for_parquet_file(default_path: str) -> str:
    """Prompt user for parquet file path if needed."""
    
    # Check if default file exists
    if os.path.exists(default_path):
        while True:
            response = input(f"\n📁 Default parquet file found: {default_path}\n   Use this file? (y/n): ").strip().lower()
            if response in ['y', 'yes']:
                return default_path
            elif response in ['n', 'no']:
                break
            else:
                print("   Please enter 'y' for yes or 'n' for no.")
    else:
        print(f"\n📁 Default parquet file not found: {default_path}")
    
    # Prompt for file path
    while True:
        file_path = input("\n📝 Enter the path to your parquet file: ").strip()
        
        if not file_path:
            print("   Please enter a valid file path.")
            continue
            
        # Remove quotes if user wrapped the path
        file_path = file_path.strip('"\'')
        
        if os.path.exists(file_path):
            if file_path.lower().endswith('.parquet'):
                return file_path
            else:
                print("   Warning: File doesn't have .parquet extension, but proceeding...")
                return file_path
        else:
            print(f"   File not found: {file_path}")
            retry = input("   Try again? (y/n): ").strip().lower()
            if retry not in ['y', 'yes']:
                print("   Exiting...")
                exit(1)


class AzureStorageParquetCleaner:
    """Azure Storage parquet file cleanup utility."""
    
    def __init__(self, parquet_file_path: str, target_containers: Optional[List[str]] = None):
        self.blob_service_client: Optional[BlobServiceClient] = None
        self.parquet_file_path = parquet_file_path
        self.target_containers = target_containers
        self.azure_filenames: Set[str] = set()
    
    def connect_with_connection_string(self, connection_string: str) -> bool:
        """Connect using connection string."""
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            # Test connection
            next(iter(self.blob_service_client.list_containers()), None)
            print("✅ Connected to Azure Storage using connection string")
            return True
        except Exception as e:
            print(f"❌ Failed to connect with connection string: {e}")
            return False
    
    def connect_with_account_key(self, account_name: str, account_key: str) -> bool:
        """Connect using storage account name and key."""
        try:
            account_url = f"https://{account_name}.blob.core.windows.net"
            self.blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=account_key
            )
            # Test connection
            next(iter(self.blob_service_client.list_containers()), None)
            print(f"✅ Connected to Azure Storage account: {account_name}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect with account key: {e}")
            return False
    
    def connect_with_managed_identity(self, account_url: str, client_id: Optional[str] = None) -> bool:
        """Connect using managed identity or DefaultAzureCredential."""
        try:
            if client_id:
                credential = DefaultAzureCredential(managed_identity_client_id=client_id)
                print(f"Using managed identity with client ID: {client_id}")
            else:
                credential = DefaultAzureCredential()
                print("Using DefaultAzureCredential (managed identity, service principal, or Azure CLI)")
            
            self.blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=credential
            )
            # Test connection
            next(iter(self.blob_service_client.list_containers()), None)
            print(f"✅ Connected to Azure Storage: {account_url}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect with managed identity: {e}")
            return False
    
    def collect_azure_filenames(self) -> bool:
        """Collect all filenames from Azure Storage across all containers."""
        if not self.blob_service_client:
            print("❌ Not connected to Azure Storage")
            return False
        
        print("🔍 Collecting filenames from Azure Storage...")
        try:
            # List all containers
            all_containers = list(self.blob_service_client.list_containers())
            
            # Filter containers if specific ones are requested
            if self.target_containers:
                containers = [c for c in all_containers if c.name in self.target_containers]
                print(f"📁 Found {len(all_containers)} total containers, targeting {len(containers)} specified containers")
                print(f"🎯 Target containers: {', '.join(self.target_containers)}")
                
                # Check if all requested containers exist
                found_container_names = {c.name for c in containers}
                missing_containers = set(self.target_containers) - found_container_names
                if missing_containers:
                    print(f"⚠️  Warning: Containers not found: {', '.join(missing_containers)}")
            else:
                containers = all_containers
                print(f"📁 Found {len(containers)} containers (scanning all)")
            
            total_files = 0
            for container in containers:
                container_name = container.name
                try:
                    container_client = self.blob_service_client.get_container_client(container_name)
                    blobs = list(container_client.list_blobs())
                    
                    for blob in blobs:
                        # Extract just the filename (without path)
                        filename = os.path.basename(blob.name)
                        if filename:  # Skip empty filenames
                            self.azure_filenames.add(filename)
                    
                    print(f"   {container_name}: {len(blobs)} files")
                    total_files += len(blobs)
                    
                except Exception as e:
                    print(f"   ❌ Error accessing container '{container_name}': {e}")
                    continue
            
            print(f"📊 Total files found in Azure Storage: {total_files}")
            print(f"📊 Unique filenames collected: {len(self.azure_filenames)}")
            
            # Show sample of collected filenames
            if self.azure_filenames:
                sample_files = list(self.azure_filenames)[:10]
                print(f"📄 Sample filenames: {', '.join(sample_files)}")
                if len(self.azure_filenames) > 10:
                    print(f"   ... and {len(self.azure_filenames) - 10} more")
            
            return len(self.azure_filenames) > 0
            
        except Exception as e:
            print(f"❌ Error collecting Azure Storage filenames: {e}")
            return False
    
    def process_parquet_file(self, dry_run: bool = False) -> bool:
        """Process the parquet file to remove rows with matching filenames."""
        # Check if parquet file exists
        if not os.path.exists(self.parquet_file_path):
            print(f"❌ Parquet file not found: {self.parquet_file_path}")
            return False
        
        try:
            print(f"📖 Reading parquet file: {self.parquet_file_path}")
            df = pd.read_parquet(self.parquet_file_path)
            
            print(f"📊 Original data shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            
            # Check if FileName column exists
            if 'FileName' not in df.columns:
                print("❌ 'FileName' column not found in the parquet file.")
                print(f"Available columns: {list(df.columns)}")
                return False
            
            # Find matching rows
            print("🔍 Finding rows with filenames that match Azure Storage files...")
            
            # Extract just the filename from the FileName column (in case it contains paths)
            df['BaseFileName'] = df['FileName'].apply(lambda x: os.path.basename(x) if pd.notna(x) else '')
            
            # Find matches
            matching_mask = df['BaseFileName'].isin(self.azure_filenames)
            matching_rows = df[matching_mask]
            
            print(f"🎯 Found {len(matching_rows)} rows with filenames matching Azure Storage files")
            
            if len(matching_rows) == 0:
                print("ℹ️  No matching rows found. No changes needed.")
                return True
            
            # Show sample of matching filenames
            matching_filenames = matching_rows['BaseFileName'].unique()
            sample_matches = list(matching_filenames)[:10]
            print(f"📄 Sample matching filenames: {', '.join(sample_matches)}")
            if len(matching_filenames) > 10:
                print(f"   ... and {len(matching_filenames) - 10} more")
            
            if dry_run:
                print("🔍 DRY RUN: Would remove the above rows. Use --execute to perform the actual removal.")
                return True
            
            # Remove matching rows
            df_filtered = df[~matching_mask].drop(columns=['BaseFileName'])
            
            print(f"📊 After removal data shape: {df_filtered.shape}")
            print(f"🗑️  Removed {len(df) - len(df_filtered)} rows")
            
            # Create backup
            backup_path = self.parquet_file_path + f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            print(f"💾 Creating backup at: {backup_path}")
            df.drop(columns=['BaseFileName']).to_parquet(backup_path, index=False)
            
            # Save updated file
            print(f"💾 Updating parquet file: {self.parquet_file_path}")
            df_filtered.to_parquet(self.parquet_file_path, index=False)
            
            print("✅ Successfully removed matching rows and updated parquet file")
            return True
            
        except Exception as e:
            print(f"❌ Error processing parquet file: {e}")
            return False
    
    def show_parquet_info(self) -> None:
        """Show information about the parquet file."""
        if not os.path.exists(self.parquet_file_path):
            print(f"❌ Parquet file not found: {self.parquet_file_path}")
            return
        
        try:
            df = pd.read_parquet(self.parquet_file_path)
            print(f"\n📊 Parquet file information:")
            print(f"   File: {self.parquet_file_path}")
            print(f"   Shape: {df.shape} (rows, columns)")
            print(f"   Columns: {list(df.columns)}")
            
            if 'FileName' in df.columns:
                unique_filenames = df['FileName'].nunique()
                print(f"   Unique FileNames: {unique_filenames}")
                
                # Show sample filenames
                sample_filenames = df['FileName'].dropna().unique()[:5]
                print(f"   Sample FileNames: {list(sample_filenames)}")
            
        except Exception as e:
            print(f"❌ Error reading parquet file: {e}")


def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(
        description='Remove rows from parquet file where FileName matches files in Azure Storage',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using connection string
  python remove_azure_matching_rows.py --connection-string "DefaultEndpointsProtocol=https;..."
  
  # Using account name and key
  python remove_azure_matching_rows.py --account-name mystorageaccount --account-key mykey123
  
  # Using managed identity
  python remove_azure_matching_rows.py --account-url https://mystorageaccount.blob.core.windows.net --use-managed-identity
  
  # Dry run to see what would be removed
  python remove_azure_matching_rows.py --connection-string "..." --dry-run
  
  # Target specific containers only  
  python remove_azure_matching_rows.py --connection-string "..." --containers "manuals,edg-manuals" --dry-run
  
  # Prompt for parquet file interactively
  python remove_azure_matching_rows.py --connection-string "..." --prompt-file --dry-run
        """
    )
    
    # Authentication options (mutually exclusive)
    auth_group = parser.add_mutually_exclusive_group(required=True)
    auth_group.add_argument('--connection-string', 
                           help='Azure Storage connection string')
    auth_group.add_argument('--account-name', 
                           help='Azure Storage account name (requires --account-key)')
    auth_group.add_argument('--account-url', 
                           help='Azure Storage account URL (for managed identity)')
    
    parser.add_argument('--account-key', 
                       help='Azure Storage account key (used with --account-name)')
    parser.add_argument('--use-managed-identity', action='store_true',
                       help='Use managed identity authentication (with --account-url)')
    parser.add_argument('--client-id', 
                       help='Managed identity client ID (optional)')
    
    # File options
    parser.add_argument('--parquet-file', 
                       default=r'c:\temp\draw_supplement_reportFilePaths.parquet',
                       help='Path to the parquet file (default: c:\\temp\\draw_supplement_reportFilePaths.parquet)')
    parser.add_argument('--prompt-file', action='store_true',
                       help='Prompt for parquet file path interactively')
    parser.add_argument('--containers', 
                       help='Comma-separated list of container names to scan (default: scan all containers)')
    
    # Operation options
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be removed without making changes')
    parser.add_argument('--execute', action='store_true',
                       help='Execute the removal (required for actual changes)')
    parser.add_argument('--info-only', action='store_true',
                       help='Only show parquet file information')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.account_name and not args.account_key:
        parser.error("--account-name requires --account-key")
    
    if args.account_url and not args.use_managed_identity:
        parser.error("--account-url requires --use-managed-identity")
    
    if not args.dry_run and not args.execute and not args.info_only:
        print("⚠️  No operation specified. Use --dry-run to preview or --execute to perform removal, or --info-only to show file info.")
        return
    
    # Handle parquet file path - prompt if requested or if default doesn't exist and no custom path provided
    if args.prompt_file or (args.parquet_file == r'c:\temp\draw_supplement_reportFilePaths.parquet' and not os.path.exists(args.parquet_file)):
        args.parquet_file = prompt_for_parquet_file(args.parquet_file)
    
    # Parse container list if provided
    target_containers = None
    if args.containers:
        target_containers = [c.strip() for c in args.containers.split(',') if c.strip()]
        print(f"🎯 Will target containers: {', '.join(target_containers)}")
    
    # Create the cleaner instance
    cleaner = AzureStorageParquetCleaner(args.parquet_file, target_containers)
    
    # Show parquet info if requested
    if args.info_only:
        cleaner.show_parquet_info()
        return
    
    print(f"🚀 Azure Storage Parquet Cleaner")
    print(f"📁 Target parquet file: {args.parquet_file}")
    
    # Connect to Azure Storage
    connected = False
    if args.connection_string:
        connected = cleaner.connect_with_connection_string(args.connection_string)
    elif args.account_name and args.account_key:
        connected = cleaner.connect_with_account_key(args.account_name, args.account_key)
    elif args.account_url and args.use_managed_identity:
        connected = cleaner.connect_with_managed_identity(args.account_url, args.client_id)
    
    if not connected:
        print("❌ Failed to connect to Azure Storage. Exiting.")
        return
    
    # Collect Azure filenames
    if not cleaner.collect_azure_filenames():
        print("❌ Failed to collect filenames from Azure Storage. Exiting.")
        return
    
    # Process the parquet file
    dry_run = args.dry_run or not args.execute
    success = cleaner.process_parquet_file(dry_run=dry_run)
    
    if success:
        if dry_run and not args.dry_run:
            print("\n⚠️  Use --execute flag to perform the actual removal.")
        print("✅ Operation completed successfully!")
    else:
        print("❌ Operation failed!")


if __name__ == "__main__":
    main()