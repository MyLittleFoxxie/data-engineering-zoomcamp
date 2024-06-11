import pandas as pd
import os
import argparse as ap
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import requests

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'

    # Step 1: Create the SQLAlchemy engine
    db_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(db_url)

    # Step 2: Get the parquet file
    response = requests.get(url)
    with open(parquet_name, 'wb') as f:
        f.write(response.content)

    # Step 3: Read the Parquet file
    trips = pq.read_table(parquet_name)
    trips = trips.to_pandas()

    # Step 4: Generate the schema
    schema = pd.io.sql.get_schema(trips, name=table_name, con=engine)

    # Step 5: Execute the schema as a raw SQL command
    with engine.connect() as connection:
        connection.execute(text(schema))

    # Step 6: Insert data into the PostgreSQL table in chunks with progress tracking
    chunk_size = 10000
    num_chunks = (len(trips) - 1) // chunk_size + 1

    for i, chunk in enumerate(range(0, len(trips), chunk_size)):
        trips.iloc[chunk:chunk + chunk_size].to_sql(table_name, engine, if_exists='append', index=False)
        print(f'Inserted chunk {i+1} of {num_chunks}')

if __name__ == '__main__':
    parser = ap.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the parquet file')

    args = parser.parse_args()
    main(args)
