import pyarrow.orc as orc
import pandas as pd

# Read ORC file into a Table
orc_file_path = '/home/swatiw/Documents/PythonWorkspace/file.orc'
orc_table = orc.read_table(orc_file_path)

# Convert Table to Pandas DataFrame
df = orc_table.to_pandas()

# Write DataFrame to CSV file
csv_file_path = '/home/swatiw/Documents/PythonWorkspace/file2.csv'
df.to_csv(csv_file_path, index=False)
