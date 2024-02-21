import os
import csv
from avro import schema, datafile, io
from io import StringIO  # Import StringIO from the correct module

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # Use StringIO to handle data as a string
    csv_reader = csv.reader(StringIO(unicode_csv_data.read()), dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [cell for cell in row]

def read_avro_schema(avro_schema_file):
    with open(avro_schema_file, 'r') as schema_file:
        return schema.parse(schema_file.read())

def process_row(row):
    return {
        "ts": float(row[0]),
        "device": row[1],
        "co": float(row[2]),
        "humidity": float(row[3]),
        "light": bool(row[4]),
        "lpg": float(row[5]),
        "motion": bool(row[6]),
        "smoke": float(row[7]),
        "temp": float(row[8])
    }

def main():
    avro_schema = read_avro_schema("/home/swatiw/Documents/PythonWorkspace/bq_table_avro.avsc")

    # Directly set paths within the code
    SOURCE_FILE_NAME = '/home/swatiw/Documents/PythonWorkspace/input.csv'
    TARGET_FILE_NAME = '/home/swatiw/Documents/PythonWorkspace/target.avro'

    with open(SOURCE_FILE_NAME, 'r', encoding='utf-8') as csvfile:  # Specify encoding
        reader = unicode_csv_reader(csvfile, delimiter=',')
        
        # Skip the header row
        next(reader)
        
        with datafile.DataFileWriter(open(TARGET_FILE_NAME, "wb"), io.DatumWriter(), avro_schema, codec='deflate') as writer:
            for count, row in enumerate(reader):
                print(f"Processing row {count}: {row}")
                try:
                    writer.append(process_row(row))
                except IndexError:
                    print("Bad record, skipping.")

    print(f"Conversion completed. Avro file saved at: {TARGET_FILE_NAME}")

if __name__ == "__main__":
    main()
