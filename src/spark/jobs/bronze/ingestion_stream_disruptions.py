import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "idfm-disruptions-raw")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
KAFKA_GROUP_ID_PREFIX = os.getenv(
    "KAFKA_GROUP_ID_PREFIX", "spark-idfm-disruptions-iceberg-writer"
)

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "60 seconds")

NESSIE_CATALOG = "nessie"
NESSIE_NAMESPACE = os.getenv("NESSIE_NAMESPACE", "bronze")
DEST_TABLE = os.getenv(
    "DESTINATION_TABLE", f"{NESSIE_CATALOG}.{NESSIE_NAMESPACE}.disruptions"
)


BATCH_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

PAYLOAD_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("payload_type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("fetched_at", StringType(), True),
        StructField("event_date", StringType(), True),
        StructField("batch_time", StringType(), True),
        StructField("raw_json", StringType(), True),
    ]
)


def ensure_target_table(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    spark.sql(
        f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{NESSIE_NAMESPACE}"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
          event_id STRING,
          payload_type STRING,
          source STRING,
          fetched_at STRING,
          event_date STRING,
          batch_time TIMESTAMP,
          raw_json STRING,
          processing_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(batch_time))
        TBLPROPERTIES (
          'format-version' = '2',
          'write.format.default' = 'parquet'
        )
        """
    )


def build_stream(spark: SparkSession):
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("groupIdPrefix", KAFKA_GROUP_ID_PREFIX)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.select(
            F.from_json(
                F.col("value").cast("string"), PAYLOAD_SCHEMA
            ).alias("payload")
        )
        .where(F.col("payload").isNotNull())
        .select(
            F.col("payload.event_id").alias("event_id"),
            F.col("payload.payload_type").alias("payload_type"),
            F.col("payload.source").alias("source"),
            F.col("payload.fetched_at").alias("fetched_at"),
            F.col("payload.event_date").alias("event_date"),
            F.to_timestamp(
                F.col("payload.batch_time"), BATCH_TIME_FORMAT
            ).alias("batch_time"),
            F.col("payload.raw_json").alias("raw_json"),
            F.current_timestamp().alias("processing_time"),
        )
    )

    return parsed_df


def main() -> None:
    if not KAFKA_BOOTSTRAP_SERVERS:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set")
    if not CHECKPOINT_LOCATION:
        raise ValueError("CHECKPOINT_LOCATION must be set")

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_target_table(spark)

    stream_df = build_stream(spark)

    query = (
        stream_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .option("fanout-enabled", "true")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .toTable(DEST_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
