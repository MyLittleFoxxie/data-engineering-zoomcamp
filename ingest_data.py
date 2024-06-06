import pandas as pd
import argparse as ap
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url= params.url

    # Step 1: Create the SQLAlchemy engine
    db_url = 'postgresql://{user}:{password}@{host}:{port}/{tb}'
    engine = create_engine(db_url)

    # Step 2: Read the Parquet file
    trips = pq.read_table('yellow_tripdata_2024-01.parquet')
    trips = trips.to_pandas()

    # Step 3: Generate the schema
    schema = pd.io.sql.get_schema(trips, name='yellow_taxi_data', con=engine)

    # Step 4: Execute the schema as a raw SQL command
    with engine.connect() as connection:
        connection.execute(text(schema))

    # Step 5: Insert data into the PostgreSQL table in chunks with progress tracking
    chunk_size = 10000
    num_chunks = (len(trips) - 1) // chunk_size + 1

    for i, chunk in enumerate(range(0, len(trips), chunk_size)):
        trips[chunk:chunk + chunk_size].to_sql('yellow_taxi_data', engine, if_exists='append', index=False)
        print(f'Inserted chunk {i+1} of {num_chunks}')


    parser = ap.ArgumentParser('user', help=' user name for postgres')
    parser = ap.ArgumentParser('password', help='password for postgres')
    parser = ap.ArgumentParser('host', help='host for postgres')
    parser = ap.ArgumentParser('port', help='port for postgres')
    parser = ap.ArgumentParser('db', help='database name for postgres')
    parser = ap.ArgumentParser('table_name', help='name of the table where we will write the results to')
    parser = ap.ArgumentParser('url', help='url of the csv file')
