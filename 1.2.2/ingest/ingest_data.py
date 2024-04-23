import pandas as pd
import pyarrow.parquet as pq
import argparse
import logging
from sqlalchemy import create_engine
from time import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(params):
    engine = create_engine(f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}')
    
    tripdata_parquet_fileclass = pq.ParquetFile(params.filename)
    
    count = 0
    total_time = 0
    try:
        with engine.connect() as connection:
            for batch in tripdata_parquet_fileclass.iter_batches(params.batch_size):
                upload_start = time()
                tripdata_batch_df = batch.to_pandas()
                tripdata_batch_df.to_sql(name=params.table_name, con=connection, if_exists='append', index=False)
                upload_time = time() - upload_start
                total_time += upload_time
                count += 1
                logging.info(f'Chunk {count} upload complete! Took {upload_time:.3f} seconds.')
    except Exception as e:
        logging.error("Failed to upload data", exc_info=True)
        raise e
    finally:
        logging.info(f"Total upload time: {total_time:.3f} seconds for {count} chunks. Upload complete!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to PostgreSQL')
    parser.add_argument('--user', required=True, help='User name for PostgreSQL')
    parser.add_argument('--password', required=True, help='Password for PostgreSQL')
    parser.add_argument('--host', required=True, help='Host for PostgreSQL')
    parser.add_argument('--port', required=True, help='Port for PostgreSQL')
    parser.add_argument('--db', required=True, help='Database name for PostgreSQL')
    parser.add_argument('--table_name', required=True, help='Name of the table to write results to')
    parser.add_argument('--filename', required=True, help='Name of the parquet file to ingest')
    parser.add_argument('--batch_size', type=int, default=100000, help='Number of records per batch')

    args = parser.parse_args()
    main(args)
