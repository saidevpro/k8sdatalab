import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "idfm-disruptions-raw")
NESSIE_URI = os.getenv("NESSIE_URI")
NESSIE_REF = os.getenv("NESSIE_REF", "main")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60000)
    env.set_parallelism(2)

    table_env = StreamTableEnvironment.create(env)

    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval",
        "60s",
    )

    table_env.execute_sql(
        f"""
        CREATE CATALOG nessie WITH (
          'type' = 'iceberg',
          'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
          'uri' = '{NESSIE_URI}',
          'ref' = '{NESSIE_REF}',
          'warehouse' = '{WAREHOUSE}',
          'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
          's3.endpoint' = '{S3_ENDPOINT}',
          's3.path-style-access' = 'true',
          'client.region' = 'us-east-1',
          's3.region' = 'us-east-1',
          's3.access-key-id' = '{S3_ACCESS_KEY}',
          's3.secret-access-key' = '{S3_SECRET_KEY}'
        )
        """
    )

    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS nessie.bronze")

    table_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS nessie.bronze.disruptions (
          event_id STRING,
          payload_type STRING,
          source STRING,
          fetched_at STRING,
          event_date STRING,
          raw_json STRING,
          processing_time TIMESTAMP(3)
        )
        PARTITIONED BY (event_date)
        WITH (
          'format-version' = '2',
          'write.format.default' = 'parquet'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE kafka_disruptions_raw (
          event_id STRING,
          payload_type STRING,
          source STRING,
          fetched_at STRING,
          event_date STRING,
          raw_json STRING
        )
        WITH (
          'connector' = 'kafka',
          'topic' = '{KAFKA_TOPIC}',
          'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
          'properties.group.id' = 'flink-idfm-disruptions-iceberg-writer',
          'scan.startup.mode' = 'latest-offset',
          'format' = 'json',
          'json.ignore-parse-errors' = 'true'
        )
        """
    )

    table_env.execute_sql(
        """
        INSERT INTO nessie.bronze.disruptions
        SELECT
          event_id,
          payload_type,
          source,
          fetched_at,
          event_date,
          raw_json,
          CURRENT_TIMESTAMP
        FROM kafka_disruptions_raw
        """
    ).wait()


if __name__ == "__main__":
    main()
