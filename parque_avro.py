import pyarrow.parquet as pq
import fastavro
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

def parquet_to_avro(parquet_file_path, avro_file_path, avro_schema):
    # Read Parquet file
    parquet_table = pq.read_table(parquet_file_path)

    # Convert Parquet table to Pandas DataFrame
    pandas_df = parquet_table.to_pandas()

    # Convert Pandas DataFrame to a list of dictionaries
    data = pandas_df.to_dict(orient='records')

    # Write Avro file
    with open(avro_file_path, 'wb') as avro_file:
        # Using DataFileWriter from avro
        writer = DataFileWriter(avro_file, DatumWriter(), avro_schema)
        for record in data:
            writer.append(record)
        writer.close()

# Example usage
parquet_file_path = '/home/swatiw/Documents/PythonWorkspace/input.parquet'
avro_file_path = '/home/swatiw/Documents/PythonWorkspace/p_avro.avro'
avro_schema = avro.schema.parse(open('/home/swatiw/Documents/PythonWorkspace/bq_table_avro.avsc', 'r').read())

parquet_to_avro(parquet_file_path, avro_file_path, avro_schema)
