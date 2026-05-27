FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir \
    kafka-python \
    cassandra-driver \
    influxdb-client \
    requests \
    pandas \
    great-expectations==0.18.15