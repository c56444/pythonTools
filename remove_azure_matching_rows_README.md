# Azure Storage Parquet File Cleanup Script

## Overview

`remove_azure_matching_rows.py` is a Python script that connects to an Azure Storage account, scans specified containers for files, and removes rows from a local parquet file where the `FileName` column matches files found in Azure Storage.

This tool is particularly useful for:
- Cleaning up parquet files by removing entries for files that already exist in Azure Storage
- Synchronizing local file inventories with cloud storage
- Managing document catalogs and preventing duplicate file tracking

## Features

- ✅ **Multiple Authentication Methods**: Connection string, account key, or managed identity
- ✅ **Container Filtering**: Target specific containers instead of scanning all
- ✅ **Interactive File Selection**: Prompt for parquet file path when needed
- ✅ **Safe Operations**: Dry-run mode and automatic backups
- ✅ **Flexible Matching**: Handles both full paths and basenames in filename matching
- ✅ **Comprehensive Logging**: Detailed output showing what's happening at each step
- ✅ **Error Handling**: Graceful handling of connection issues and missing files

## Prerequisites

### Required Python Packages
```bash
pip install azure-storage-blob azure-identity pandas pyarrow
```

### Azure Storage Account Access
You need one of the following:
- **Connection String**: Full Azure Storage connection string
- **Account Name + Key**: Storage account name and access key
- **Managed Identity**: For Azure-hosted environments with proper RBAC

## Installation

1. Ensure you have Python 3.7+ installed
2. Install required packages:
   ```bash
   pip install azure-storage-blob azure-identity pandas pyarrow
   ```
3. Download the script: `remove_azure_matching_rows.py`

## Usage

### Basic Syntax
```bash
python remove_azure_matching_rows.py [authentication_options] [operation_options]
```

### Authentication Options

#### Using Connection String
```bash
python remove_azure_matching_rows.py --connection-string "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
```

#### Using Account Name and Key
```bash
python remove_azure_matching_rows.py --account-name myaccount --account-key mykey123
```

#### Using Managed Identity
```bash
python remove_azure_matching_rows.py --account-url https://myaccount.blob.core.windows.net --use-managed-identity
```

### Operation Options

| Option | Description | Default |
|--------|-------------|---------|
| `--parquet-file` | Path to parquet file | `c:\temp\draw_supplement_reportFilePaths.parquet` |
| `--prompt-file` | Prompt for parquet file path interactively | - |
| `--containers` | Comma-separated container names | All containers |
| `--dry-run` | Preview changes without executing | - |
| `--execute` | Execute the removal | - |
| `--info-only` | Show parquet file info only | - |

## Examples

### 1. View Parquet File Information
```bash
python remove_azure_matching_rows.py --connection-string "..." --info-only
```

### 2. Dry Run - Preview What Would Be Removed
```bash
# Check all containers
python remove_azure_matching_rows.py --account-name myaccount --account-key mykey --dry-run

# Check specific containers only
python remove_azure_matching_rows.py --account-name myaccount --account-key mykey --containers "edg-manuals,documents" --dry-run
```

### 3. Execute Removal
```bash
# Remove rows matching files in specific containers
python remove_azure_matching_rows.py --account-name myaccount --account-key mykey --containers "edg-manuals" --execute

# Remove rows matching files in all containers
python remove_azure_matching_rows.py --account-name myaccount --account-key mykey --execute
```

### 4. Custom Parquet File
```bash
python remove_azure_matching_rows.py --connection-string "..." --parquet-file "c:\custom\path\myfile.parquet" --containers "documents" --dry-run
```

### 5. Interactive File Selection
```bash
# Prompt user to select parquet file interactively
python remove_azure_matching_rows.py --connection-string "..." --prompt-file --containers "edg-manuals" --dry-run

# Auto-prompt if default file doesn't exist or you want to use a different one
python remove_azure_matching_rows.py --account-name myaccount --account-key mykey --dry-run
```

## Interactive File Selection

The script can prompt you to select a parquet file interactively:

### **Automatic Prompting**
When the default file (`c:\temp\draw_supplement_reportFilePaths.parquet`) doesn't exist, the script automatically prompts:
```
📁 Default parquet file not found: c:\temp\draw_supplement_reportFilePaths.parquet

📝 Enter the path to your parquet file: 
```

### **Manual Prompting**  
Use `--prompt-file` to always prompt for file selection:
```bash
python remove_azure_matching_rows.py --connection-string "..." --prompt-file --dry-run
```

### **File Selection Flow**
```
📁 Default parquet file found: c:\temp\draw_supplement_reportFilePaths.parquet
   Use this file? (y/n): n

📝 Enter the path to your parquet file: c:\backup\my_data.parquet
```

The prompt will:
- ✅ **Validate file exists** before proceeding
- ✅ **Remove quotes** if you wrap the path
- ✅ **Warn about extensions** (but still proceed)
- ✅ **Allow retry** if file not found

## Real-World Example

Based on testing with the `satxtavopenaiprod` storage account:

```bash
# First, see what's in your parquet file
python remove_azure_matching_rows.py --account-name satxtavopenaiprod --account-key yourkey --info-only

# Check which containers have your files (dry run)
python remove_azure_matching_rows.py --account-name satxtavopenaiprod --account-key yourkey --containers "edg-manuals" --dry-run

# Execute removal for files found in edg-manuals container
python remove_azure_matching_rows.py --account-name satxtavopenaiprod --account-key yourkey --containers "edg-manuals" --execute
```

## How It Works

1. **Connect** to Azure Storage using provided authentication
2. **Prompt for file** (if requested or default doesn't exist)
3. **List containers** (all or specified ones)
4. **Collect filenames** from target containers (extracts basename from blob paths)
5. **Load parquet file** and check for `FileName` column
6. **Match filenames** between parquet and Azure Storage
7. **Create backup** of original parquet file (with timestamp)
8. **Remove matching rows** and save updated parquet file

## Output Example

```
🚀 Azure Storage Parquet Cleaner
📁 Target parquet file: c:\temp\draw_supplement_reportFilePaths.parquet
✅ Connected to Azure Storage account: satxtavopenaiprod
🔍 Collecting filenames from Azure Storage...
📁 Found 16 total containers, targeting 1 specified containers
🎯 Target containers: edg-manuals
   edg-manuals: 63581 files
📊 Total files found in Azure Storage: 63581
📊 Unique filenames collected: 63032
📖 Reading parquet file: c:\temp\draw_supplement_reportFilePaths.parquet
📊 Original data shape: (2875, 11)
🔍 Finding rows with filenames that match Azure Storage files...
🎯 Found 2875 rows with filenames matching Azure Storage files
🔍 DRY RUN: Would remove the above rows. Use --execute to perform the actual removal.
✅ Operation completed successfully!
```

## Safety Features

### Automatic Backups
Before making any changes, the script creates a timestamped backup:
```
original_file.parquet.backup_20240506_143022
```

### Dry Run Mode
Always test with `--dry-run` first to see what would be removed:
```bash
python remove_azure_matching_rows.py --connection-string "..." --dry-run
```

### Container Validation
The script warns about invalid container names:
```
⚠️  Warning: Containers not found: invalid-container
```

## Expected File Structure

Your parquet file must contain a `FileName` column. Example structure:
```
Columns: ['Cage', 'DocNum', 'DocType', 'DocumentRev', 'ECCN', 'FileName', 'FilePath', 'ReleaseDate', 'Status', 'Title', 'ptrDocumentMaster']
```

## Troubleshooting

### Connection Issues
```bash
❌ Failed to connect with connection string: Session.request() got an unexpected keyword argument 'max_results'
```
**Solution**: Update Azure SDK packages:
```bash
pip install --upgrade azure-storage-blob azure-identity
```

### Missing FileName Column
```bash
❌ 'FileName' column not found in the parquet file.
Available columns: ['Col1', 'Col2', 'Col3']
```
**Solution**: Ensure your parquet file has a `FileName` column with the filenames to match.

### No Files Found
```bash
ℹ️  No matching rows found. No changes needed.
```
**Possible reasons**:
- Files in parquet don't exist in specified Azure Storage containers
- Container names are incorrect
- Filename formats don't match (check basename vs full path)

### Authentication Errors
```bash
❌ Failed to connect with account key: (403) Server failed to authenticate the request
```
**Solution**: Verify your account name and key are correct.

## Container Strategy

Based on your storage account structure, you might want to target specific containers:

- **`edg-manuals`**: Contains PDF documents (63,581 files)
- **`manuals`**: Contains XML/image files (429,076 files)  
- **`database`**: Contains parquet backups (15 files)
- **`hr-plugin`**: Contains plugin files (151 files)

Choose containers based on your cleanup goals:
```bash
# Remove rows for PDF documents only
--containers "edg-manuals"

# Remove rows for multiple document types
--containers "edg-manuals,manuals"

# Exclude the main document container
--containers "database,hr-plugin,stage"
```

## Command Line Help

```bash
python remove_azure_matching_rows.py --help
```

This will display all available options and examples.

## License

This script is provided as-is for utility purposes. Modify and use according to your needs.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Run with `--dry-run` first to understand behavior
3. Use `--info-only` to verify parquet file structure
4. Check Azure Storage account permissions and connectivity