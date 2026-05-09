FROM ghcr.io/mlflow/mlflow:v3.7.0

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary \
    boto3

USER 1000
