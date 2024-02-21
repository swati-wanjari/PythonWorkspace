import pyarrow.orc as orc
import fastavro
from avro import schema as avro_schema
from avro import datafile, io
from io import BytesIO

def orc_to_avro(orc_file_path, avro_file_path):
    # Read ORC file
    orc_table = orc.read_table(orc_file_path)

    # Convert ORC table to Pandas DataFrame
    pandas_df = orc_table.to_pandas()

    # Convert Pandas DataFrame to a list of dictionaries
    data = pandas_df.to_dict(orient='records')

    # Read Avro schema
    avro_schema = read_avro_schema("/home/swatiw/Documents/PythonWorkspace/bq_table_avro.avsc")

    # Write Avro file
    with open(avro_file_path, 'wb') as avro_file:
        avro_writer = datafile.DataFileWriter(avro_file, io.DatumWriter(), avro_schema, codec='deflate')
        
        for record in data:
            avro_writer.append(record)

        avro_writer.close()

def read_avro_schema(avro_schema_file):
    with open(avro_schema_file, 'r') as schema_file:
        return avro_schema.parse(schema_file.read())

# Example usage
orc_file_path = '/home/swatiw/Documents/PythonWorkspace/input.orc'
avro_file_path = '/home/swatiw/Documents/PythonWorkspace/output.avro'

orc_to_avro(orc_file_path, avro_file_path)
