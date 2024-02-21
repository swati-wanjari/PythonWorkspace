import pyarrow.orc as orc
import pyarrow.parquet as pq

# Read ORC file into a Table
orc_file_path = '/home/swatiw/Documents/PythonWorkspace/file.orc'
orc_table = orc.read_table(orc_file_path)

# Write Table to Parquet file
parquet_file_path = '/home/swatiw/Documents/PythonWorkspace/file2.parquet'
pq.write_table(orc_table, parquet_file_path)
