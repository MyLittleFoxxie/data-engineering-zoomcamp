import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

# Step 1: Read the Parquet file
trips = pq.read_table('yellow_tripdata_2024-01.parquet')
trips = trips.to_pandas()
trips

# Step 2: Create the SQLAlchemy engine
db_url = 'postgresql://root:root@localhost:5432/ny_taxi'
engine = create_engine(db_url)

# Step 3: Insert data into the PostgreSQL table in chunks with progress tracking
chunk_size = 10000  # Adjust the chunk size as needed
num_chunks = (len(trips) - 1) // chunk_size + 1  # Calculate the number of chunks

for i, chunk in enumerate(range(0, len(trips), chunk_size)):
    trips[chunk:chunk + chunk_size].to_sql('yellow_taxi_data', engine, if_exists='replace', index=False)
    print(f'Inserted chunk {i+1} of {num_chunks}')

