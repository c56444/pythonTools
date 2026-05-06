# Source - https://stackoverflow.com/a
# Posted by Uwe L. Korn, modified by community. See post 'Timeline' for change history
# Retrieved 2026-01-06, License - CC BY-SA 4.0

from pyarrow.parquet import ParquetFile
# Source is either the filename or an Arrow file handle (which could be on HDFS)
source = "C:\\Temp\\Models_20260102_2102.parquet"
ParquetFile(source).metadata
ParquetFile(source).schema
print(ParquetFile(source).schema)
# prints:
