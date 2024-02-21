import pyarrow.parquet as pq
import pyarrow.orc as orc

# Read Parquet file into a Table
parquet_file_path = '/home/swatiw/Documents/PythonWorkspace/file.parquet'
parquet_table = pq.read_table(parquet_file_path)

# Convert Table to ORC format
orc_file_path = '/home/swatiw/Documents/PythonWorkspace/file1.orc'
orc.write_table(parquet_table, orc_file_path)

