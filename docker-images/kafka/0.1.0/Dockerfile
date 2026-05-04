FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests==2.32.3 \
    confluent-kafka==2.6.0

COPY src/kafka .
