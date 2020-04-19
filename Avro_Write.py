from avro import io, datafile, schema
from os import path

if __name__ == "__main__":
    OUTFILE_NAME = 'resources\\output\\product.avro'
    INPUT_SCHEMA_NAME = 'resources\\schemas\\product.avsc'
    # Open a file
    fo = open(path.relpath(INPUT_SCHEMA_NAME), "r+")
    SCHEMA_STR = fo.read();
    #print("Read String is : ", SCHEMA_STR)
    # Close opend file
    fo.close()

    SCHEMA = schema.Parse(SCHEMA_STR)

    # Create a 'record' (datum) writer
    rec_writer = io.DatumWriter(SCHEMA)

    # Create a 'data file' (avro file) writer
    df_writer = datafile.DataFileWriter(
        open(path.relpath(OUTFILE_NAME), 'wb'),
        rec_writer,
        writer_schema=SCHEMA
    )

    for i in range(100000):
        df_writer.append({"product_id":1000+i,"product_name":"Hugo Boss XY{}".format(i),"product_status": "AVAILABLE","price":10.35+i})

    df_writer.close()