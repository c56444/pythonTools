# Pandas DataFrame Row Deletion - Quick Reference

## 🗑️ Common Row Deletion Methods

### 1. Delete by Index Position
```python
# Delete single row
df = df.drop(0).reset_index(drop=True)

# Delete multiple rows
df = df.drop([0, 2, 4]).reset_index(drop=True)

# Delete index range
df = df.drop(df.index[2:5]).reset_index(drop=True)
```

### 2. Delete by Condition
```python
# Delete rows where column value meets condition
df = df[df['age'] <= 35].reset_index(drop=True)

# Delete with multiple conditions (AND)
df = df[(df['age'] > 25) & (df['salary'] < 100000)].reset_index(drop=True)

# Delete with multiple conditions (OR)  
df = df[(df['age'] <= 30) | (df['salary'] >= 70000)].reset_index(drop=True)
```

### 3. Delete by Specific Values
```python
# Delete rows with specific value
df = df[df['department'] != 'IT'].reset_index(drop=True)

# Delete rows with multiple values
df = df[~df['department'].isin(['IT', 'HR'])].reset_index(drop=True)

# Delete rows containing text pattern
df = df[~df['name'].str.contains('John', case=False)].reset_index(drop=True)
```

### 4. Delete Duplicates
```python
# Remove exact duplicate rows
df = df.drop_duplicates().reset_index(drop=True)

# Remove duplicates based on specific columns
df = df.drop_duplicates(subset=['name', 'email']).reset_index(drop=True)

# Keep last occurrence instead of first
df = df.drop_duplicates(keep='last').reset_index(drop=True)
```

### 5. Delete Rows with Missing Values
```python
# Delete rows with any missing values
df = df.dropna().reset_index(drop=True)

# Delete rows with missing values in specific columns
df = df.dropna(subset=['email', 'phone']).reset_index(drop=True)

# Delete only if ALL values in row are missing
df = df.dropna(how='all').reset_index(drop=True)
```

## ⚡ Quick Examples

### Example Dataset
```python
import pandas as pd
import numpy as np

df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'age': [25, 30, 35, 28, 32],
    'salary': [50000, 60000, 70000, 55000, 65000],
    'department': ['IT', 'HR', 'Finance', 'IT', 'Marketing']
})
```

### Common Deletion Scenarios
```python
# Remove employees over 30
df = df[df['age'] <= 30]

# Remove IT department  
df = df[df['department'] != 'IT']

# Remove low earners (bottom 25%)
threshold = df['salary'].quantile(0.25)
df = df[df['salary'] > threshold]

# Remove outliers (beyond 2 standard deviations)
mean_val = df['salary'].mean()
std_val = df['salary'].std()
df = df[abs(df['salary'] - mean_val) <= 2 * std_val]

# Remove specific people
df = df[~df['name'].isin(['Alice', 'Bob'])]
```

## 🔧 Best Practices

### ✅ Do This
```python
# Always reset index after deletion
df = df[condition].reset_index(drop=True)

# Make a backup before deletion
df_backup = df.copy()

# Use parentheses for complex conditions
df = df[(df['age'] > 25) & (df['salary'] < 100000)]

# Use descriptive variable names
high_earners = df[df['salary'] > 70000]
```

### ❌ Avoid This
```python
# Don't forget to reset index
df = df[condition]  # Index will have gaps

# Don't use chained conditions without parentheses
df = df[df['age'] > 25 & df['salary'] < 100000]  # Wrong!

# Don't modify original without backup
df.drop([1, 2, 3], inplace=True)  # Original data lost
```

## 🚀 Performance Tips

### For Large DataFrames
```python
# Boolean indexing is faster than drop()
# Fast
df = df[df['column'] != value]

# Slower  
df = df.drop(df[df['column'] == value].index)

# Use query() for complex string conditions
df = df.query('age > 25 and department == "IT"')

# Process in chunks for very large datasets
chunk_size = 10000
for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
    chunk_filtered = chunk[chunk['column'] > threshold]
    # Process chunk...
```

## 📊 Verification

### Check Your Deletion
```python
# Before deletion
print(f"Original shape: {df.shape}")
print(f"Original row count: {len(df)}")

# After deletion  
df_filtered = df[condition]
print(f"New shape: {df_filtered.shape}")
print(f"Deleted {len(df) - len(df_filtered)} rows")

# Verify specific conditions
print(f"Remaining unique values: {df_filtered['column'].unique()}")
```

## 🏃‍♂️ Quick Start

To run the example scripts:

```bash
# Install requirements
pip install pandas numpy

# Run comprehensive examples
python delete_dataframe_rows.py

# Run simple examples  
python simple_dataframe_deletion.py
```

## 📝 Key Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equal | `df['age'] == 30` |
| `!=` | Not equal | `df['name'] != 'John'` |
| `>`, `<` | Greater/Less than | `df['salary'] > 50000` |
| `>=`, `<=` | Greater/Less than or equal | `df['age'] >= 25` |
| `&` | AND | `(df['age'] > 25) & (df['salary'] < 100000)` |
| `\|` | OR | `(df['age'] < 25) \| (df['age'] > 65)` |
| `~` | NOT | `~df['active']` |
| `.isin()` | Value in list | `df['dept'].isin(['IT', 'HR'])` |
| `.str.contains()` | String contains pattern | `df['name'].str.contains('John')` |