import pyarrow as pa
import pyarrow.orc as orc
import pandas as pd

# Read CSV file into a DataFrame
csv_file_path = '/home/swatiw/Documents/PythonWorkspace/input.csv'
df = pd.read_csv(csv_file_path)

# Convert DataFrame to ORC format
orc_file_path = '/home/swatiw/Documents/PythonWorkspace/input.orc'

# Create a PyArrow Table from the DataFrame
table = pa.Table.from_pandas(df)

# Write the PyArrow Table to ORC
with pa.output_stream(orc_file_path) as orc_stream:
    orc.write_table(table, orc_stream)
