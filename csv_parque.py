import pandas as pd

# Read CSV file into a DataFrame
csv_file_path = '/home/swatiw/Documents/PythonWorkspace/swati.csv'
df = pd.read_csv(csv_file_path)

# Convert DataFrame to Parquet format
parquet_file_path = '/home/swatiw/Documents/PythonWorkspace/file.parquet'
df.to_parquet(parquet_file_path, engine='pyarrow')
