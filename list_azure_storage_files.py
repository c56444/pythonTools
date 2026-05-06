#!/usr/bin/env python3
"""
Azure Storage File Listing Script

This script lists files (blobs) in an Azure Storage account with support for:
- Multiple authentication methods (connection string, account key, managed identity)
- Listing all containers or specific container
- Detailed file information (name, size, last modified, content type)
- Filtering by prefix
- Export results to CSV

Required packages:
    pip install azure-storage-blob azure-identity

Usage examples:
    python list_azure_storage_files.py --connection-string "DefaultEndpointsProtocol=https;..."
    python list_azure_storage_files.py --account-name mystorageaccount --account-key mykey123
    python list_azure_storage_files.py --account-url https://mystorageaccount.blob.core.windows.net --use-managed-identity
"""

import os
import argparse
import csv
from datetime import datetime
from typing import List, Optional, Dict, Any

try:
    from azure.storage.blob import BlobServiceClient, ContainerClient
    from azure.identity import DefaultAzureCredential
    from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError
except ImportError as e:
    print(f"Error importing Azure libraries: {e}")
    print("Please install required packages: pip install azure-storage-blob azure-identity")
    exit(1)


class AzureStorageFileLister:
    """Azure Storage file listing utility with multiple authentication methods."""
    
    def __init__(self):
        self.blob_service_client: Optional[BlobServiceClient] = None
    
    def connect_with_connection_string(self, connection_string: str) -> bool:
        """Connect using connection string."""
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            # Test connection
            list(self.blob_service_client.list_containers(max_results=1))
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
            list(self.blob_service_client.list_containers(max_results=1))
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
            list(self.blob_service_client.list_containers(max_results=1))
            print(f"✅ Connected to Azure Storage: {account_url}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect with managed identity: {e}")
            return False
    
    def list_containers(self) -> List[Dict[str, Any]]:
        """List all containers in the storage account."""
        if not self.blob_service_client:
            raise ValueError("Not connected to Azure Storage")
        
        containers = []
        try:
            for container in self.blob_service_client.list_containers():
                containers.append({
                    'name': container.name,
                    'last_modified': container.last_modified,
                    'public_access': container.public_access,
                    'metadata': container.metadata or {}
                })
            print(f"📁 Found {len(containers)} containers")
            return containers
        except Exception as e:
            print(f"❌ Error listing containers: {e}")
            return []
    
    def list_files_in_container(self, container_name: str, prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all files (blobs) in a specific container."""
        if not self.blob_service_client:
            raise ValueError("Not connected to Azure Storage")
        
        files = []
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            
            # List blobs with optional prefix filter
            blob_list = container_client.list_blobs(name_starts_with=prefix)
            
            for blob in blob_list:
                files.append({
                    'container': container_name,
                    'name': blob.name,
                    'size': blob.size,
                    'size_mb': round(blob.size / (1024 * 1024), 2) if blob.size else 0,
                    'last_modified': blob.last_modified,
                    'content_type': blob.content_settings.content_type if blob.content_settings else 'Unknown',
                    'etag': blob.etag,
                    'blob_type': blob.blob_type,
                    'creation_time': blob.creation_time,
                    'metadata': blob.metadata or {}
                })
            
            prefix_info = f" with prefix '{prefix}'" if prefix else ""
            print(f"📄 Found {len(files)} files in container '{container_name}'{prefix_info}")
            return files
            
        except ResourceNotFoundError:
            print(f"❌ Container '{container_name}' not found")
            return []
        except Exception as e:
            print(f"❌ Error listing files in container '{container_name}': {e}")
            return []
    
    def list_all_files(self, prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all files across all containers."""
        all_files = []
        containers = self.list_containers()
        
        for container in containers:
            container_name = container['name']
            files = self.list_files_in_container(container_name, prefix)
            all_files.extend(files)
        
        print(f"📊 Total files found: {len(all_files)}")
        return all_files
    
    def print_file_summary(self, files: List[Dict[str, Any]]) -> None:
        """Print a summary of files."""
        if not files:
            print("No files found.")
            return
        
        total_size_mb = sum(file['size_mb'] for file in files)
        total_size_gb = round(total_size_mb / 1024, 2)
        
        print(f"\n📊 File Summary:")
        print(f"   Total files: {len(files)}")
        print(f"   Total size: {total_size_mb:.2f} MB ({total_size_gb:.2f} GB)")
        
        # Group by container
        containers = {}
        for file in files:
            container = file['container']
            if container not in containers:
                containers[container] = {'count': 0, 'size_mb': 0}
            containers[container]['count'] += 1
            containers[container]['size_mb'] += file['size_mb']
        
        print(f"\n📁 Files by container:")
        for container, stats in containers.items():
            print(f"   {container}: {stats['count']} files, {stats['size_mb']:.2f} MB")
    
    def export_to_csv(self, files: List[Dict[str, Any]], filename: str) -> None:
        """Export file list to CSV."""
        if not files:
            print("No files to export.")
            return
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['container', 'name', 'size', 'size_mb', 'last_modified', 
                            'content_type', 'blob_type', 'creation_time', 'etag']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for file in files:
                    # Convert datetime objects to strings for CSV
                    row = file.copy()
                    for key, value in row.items():
                        if isinstance(value, datetime):
                            row[key] = value.isoformat()
                        elif key == 'metadata' and value:
                            row[key] = str(value)
                    
                    # Only include fields that are in fieldnames
                    csv_row = {k: v for k, v in row.items() if k in fieldnames}
                    writer.writerow(csv_row)
            
            print(f"✅ Exported {len(files)} files to '{filename}'")
        except Exception as e:
            print(f"❌ Error exporting to CSV: {e}")


def main():
    parser = argparse.ArgumentParser(description='List files in Azure Storage account')
    
    # Authentication options
    auth_group = parser.add_mutually_exclusive_group(required=True)
    auth_group.add_argument('--connection-string', 
                           help='Azure Storage connection string')
    auth_group.add_argument('--account-key', 
                           help='Storage account key (requires --account-name)')
    auth_group.add_argument('--use-managed-identity', 
                           action='store_true',
                           help='Use managed identity or DefaultAzureCredential (requires --account-url)')
    
    # Additional authentication parameters
    parser.add_argument('--account-name', 
                        help='Storage account name (required with --account-key)')
    parser.add_argument('--account-url', 
                        help='Storage account URL (required with --use-managed-identity)')
    parser.add_argument('--client-id', 
                        help='Managed identity client ID (optional)')
    
    # Listing options
    parser.add_argument('--container', 
                        help='Specific container to list (default: all containers)')
    parser.add_argument('--prefix', 
                        help='Filter files by prefix')
    parser.add_argument('--export-csv', 
                        help='Export results to CSV file')
    parser.add_argument('--detailed', 
                        action='store_true',
                        help='Show detailed file information')
    
    # Environment variable fallbacks
    parser.add_argument('--use-env', 
                        action='store_true',
                        help='Use environment variables for authentication')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.account_key and not args.account_name:
        parser.error("--account-key requires --account-name")
    
    if args.use_managed_identity and not args.account_url:
        parser.error("--use-managed-identity requires --account-url")
    
    # Initialize the storage client
    storage_lister = AzureStorageFileLister()
    
    # Connect to Azure Storage
    connected = False
    
    if args.use_env:
        # Try environment variables
        conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if conn_str:
            connected = storage_lister.connect_with_connection_string(conn_str)
        else:
            account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
            account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
            if account_name and account_key:
                connected = storage_lister.connect_with_account_key(account_name, account_key)
    
    elif args.connection_string:
        connected = storage_lister.connect_with_connection_string(args.connection_string)
    
    elif args.account_key:
        connected = storage_lister.connect_with_account_key(args.account_name, args.account_key)
    
    elif args.use_managed_identity:
        connected = storage_lister.connect_with_managed_identity(args.account_url, args.client_id)
    
    if not connected:
        print("\n❌ Failed to connect to Azure Storage. Please check your credentials.")
        print("\nTip: You can also set these environment variables:")
        print("   AZURE_STORAGE_CONNECTION_STRING")
        print("   AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY")
        return
    
    try:
        # List files
        if args.container:
            files = storage_lister.list_files_in_container(args.container, args.prefix)
        else:
            files = storage_lister.list_all_files(args.prefix)
        
        # Display results
        if files:
            storage_lister.print_file_summary(files)
            
            if args.detailed:
                print(f"\n📄 Detailed file listing:")
                for file in files[:20]:  # Limit to first 20 for readability
                    print(f"\n   Container: {file['container']}")
                    print(f"   Name: {file['name']}")
                    print(f"   Size: {file['size_mb']:.2f} MB")
                    print(f"   Last Modified: {file['last_modified']}")
                    print(f"   Content Type: {file['content_type']}")
                    print(f"   Blob Type: {file['blob_type']}")
                
                if len(files) > 20:
                    print(f"\n   ... and {len(files) - 20} more files")
            
            # Export to CSV if requested
            if args.export_csv:
                storage_lister.export_to_csv(files, args.export_csv)
        
    except KeyboardInterrupt:
        print("\n\n🛑 Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()