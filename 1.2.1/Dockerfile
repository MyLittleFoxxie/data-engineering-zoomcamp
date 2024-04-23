# docker build -t test:pandas .
# docker run -it test:pandas

FROM python:latest

RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py"]