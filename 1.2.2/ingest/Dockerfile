# docker build -t taxi_ingest:v001 .
# docker run -it taxi_ingest:v001   --user=root   --password=root   --host=localhost   --port=5432   --db=ny_taxi   --table_name=yellow_taxi_trips   --filename=yellow_tripdata_2021-01.parquet

# Use the official Python image as the base image
FROM python:latest

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install pandas sqlalchemy pyarrow psycopg2-binary

# Set the working directory inside the container
WORKDIR /app

# Copy the Python script into the container
COPY ingest_data.py /app/

# Copy the Parquet file into the container
COPY yellow_tripdata_2021-01.parquet /app/

# Set the default command for the container
ENTRYPOINT ["python", "ingest_data.py"]
