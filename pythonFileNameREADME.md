# Parquet File Entry Remover

This package contains scripts to remove entries from the parquet file `c:\temp\draw_supplement_reportFilePaths.parquet` based on the FileName column.

## Files Created

1. **remove_parquet_entry.py** - Command line version
2. **interactive_parquet_editor.py** - Interactive GUI-like version  
3. **remove_parquet_entry.bat** - Windows batch file to run either version
4. **README.md** - This instruction file

## Prerequisites

- Python 3.6 or higher
- pandas library
- pyarrow library

The batch file will automatically install pandas and pyarrow if they're not available.

## Quick Start

### Option 1: Easy Interactive Mode (Recommended)
```bash
# Double-click on remove_parquet_entry.bat
# OR run from command prompt:
C:\temp\remove_parquet_entry.bat
# Choose option 1 for interactive mode
```

### Option 2: Command Line Mode
```bash
# List all filenames in the parquet file
python C:\temp\remove_parquet_entry.py --list

# Remove a specific filename
python C:\temp\remove_parquet_entry.py "filename_to_remove.txt"

# Or use the batch file
C:\temp\remove_parquet_entry.bat "filename_to_remove.txt"
```

## Features

### Interactive Mode Features:
- ✅ List all FileNames in the parquet file
- ✅ Search for FileNames using partial matches
- ✅ Remove specific FileNames with confirmation
- ✅ Automatic backup creation
- ✅ User-friendly interface

### Command Line Features:
- ✅ Quick removal of known filenames
- ✅ List all filenames for reference
- ✅ Automatic backup creation
- ✅ Confirmation prompts for safety

## Safety Features

- **Automatic Backup**: Creates a `.backup` copy before making changes
- **Confirmation Prompts**: Asks for confirmation before removing entries
- **Validation**: Checks if filename exists before attempting removal
- **Error Handling**: Graceful handling of missing files or columns

## Usage Examples

### Interactive Mode:
1. Run the batch file and choose option 1
2. View all filenames to see what's available
3. Search for specific patterns if needed
4. Remove entries with confirmation

### Command Line Mode:
```bash
# See all available filenames
python remove_parquet_entry.py --list

# Remove a specific file entry
python remove_parquet_entry.py "report_2024_01.xlsx"
```

## Output Information

The scripts will show:
- Original data shape (rows × columns)
- Number of matching entries found
- Details of entries being removed
- Final data shape after removal
- Backup file location

## Error Handling

The scripts handle various error scenarios:
- Missing parquet file
- Missing FileName column
- No matching entries
- Invalid file formats
- Permission issues

## Backup and Recovery

- Backups are automatically created with `.backup` extension
- To restore: rename the backup file to remove `.backup` extension
- Multiple operations create multiple backups (latest overwrites previous)

## Troubleshooting

### "Python not found" error:
- Install Python from python.org
- Make sure Python is added to your system PATH

### "pandas not found" error:
- The batch file will auto-install it
- Or manually run: `pip install pandas pyarrow`

### "Permission denied" error:
- Make sure the parquet file isn't open in another application
- Run as administrator if needed

### "FileName column not found":
- Check that your parquet file has the expected structure
- Use the list function to see available columns