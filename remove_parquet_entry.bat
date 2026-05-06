@echo off
echo ====================================
echo  Parquet File Entry Remover
echo ====================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and try again.
    pause
    exit /b 1
)

REM Check if pandas is available
python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages (pandas, pyarrow)...
    pip install pandas pyarrow
    if errorlevel 1 (
        echo ERROR: Failed to install required packages
        pause
        exit /b 1
    )
)

echo Choose an option:
echo 1. Interactive mode (recommended for beginners)
echo 2. Command line mode
echo.
set /p choice="Enter your choice (1 or 2): "

if "%choice%"=="1" (
    echo Starting interactive mode...
    python "C:\temp\interactive_parquet_editor.py"
) else if "%choice%"=="2" (
    echo.
    echo Command line usage examples:
    echo   remove_parquet_entry.bat filename.txt
    echo   remove_parquet_entry.bat --list
    echo.
    if "%1"=="" (
        echo ERROR: Please provide a filename to remove or --list to see all files
        pause
        exit /b 1
    )
    python "C:\temp\remove_parquet_entry.py" %*
) else (
    echo Invalid choice. Please run the script again.
    pause
    exit /b 1
)

pause