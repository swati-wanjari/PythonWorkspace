import pandas as pd

# Read Parquet file into a DataFrame
parquet_file_path = '/home/swatiw/Documents/PythonWorkspace/file.parquet'
df = pd.read_parquet(parquet_file_path, engine='pyarrow')

# Convert DataFrame to CSV format
csv_file_path = '/home/swatiw/Documents/PythonWorkspace/file.csv'
df.to_csv(csv_file_path, index=False)
