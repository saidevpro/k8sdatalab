import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window


SOURCE_TABLE = os.getenv("SOURCE_TABLE", "nessie.bronze.next_stop")
NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")

NEXT_STOP_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.next_stop"

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "60 seconds")
MAX_FILES_PER_MICRO_BATCH = os.getenv("MAX_FILES_PER_MICRO_BATCH", "100")


REF_SCHEMA = StructType([StructField("value", StringType(), True)])
VALUE_LIST_SCHEMA = ArrayType(REF_SCHEMA)

ESTIMATED_CALL_SCHEMA = StructType(
    [
        StructField("StopPointRef", REF_SCHEMA, True),
        StructField("ExpectedArrivalTime", StringType(), True),
        StructField("ExpectedDepartureTime", StringType(), True),
        StructField("AimedArrivalTime", StringType(), True),
        StructField("AimedDepartureTime", StringType(), True),
        StructField("ArrivalStatus", StringType(), True),
        StructField("DepartureStatus", StringType(), True),
    ]
)

CALLS_SCHEMA = StructType(
    [StructField("EstimatedCall", ArrayType(ESTIMATED_CALL_SCHEMA), True)]
)

RAW_JSON_SCHEMA = StructType(
    [
        StructField("RecordedAtTime", StringType(), True),
        StructField("LineRef", REF_SCHEMA, True),
        StructField("DatedVehicleJourneyRef", REF_SCHEMA, True),
        StructField("PublishedLineName", VALUE_LIST_SCHEMA, True),
        StructField("DirectionName", VALUE_LIST_SCHEMA, True),
        StructField("DestinationRef", REF_SCHEMA, True),
        StructField("DestinationName", VALUE_LIST_SCHEMA, True),
        StructField("OperatorRef", REF_SCHEMA, True),
        StructField("EstimatedCalls", CALLS_SCHEMA, True),
    ]
)


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    spark.sql(
        f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{SILVER_NAMESPACE}"
    )


def ensure_silver_table(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {NEXT_STOP_TABLE} (
          journey_ref         STRING,
          stop_point_ref      STRING,
          line_ref            STRING,
          published_line      STRING,
          direction           STRING,
          destination_ref     STRING,
          destination_name    STRING,
          aimed_arrival       TIMESTAMP,
          expected_arrival    TIMESTAMP,
          aimed_departure     TIMESTAMP,
          expected_departure  TIMESTAMP,
          arrival_delay_sec   BIGINT,
          departure_delay_sec BIGINT,
          arrival_status      STRING,
          departure_status    STRING,
          recorded_at         TIMESTAMP,
          batch_time          TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(batch_time))
        """
    )


def parse_bronze(stream_df):
    parsed = stream_df.withColumn(
        "raw", F.from_json(F.col("raw_json"), RAW_JSON_SCHEMA)
    )

    return parsed.select(
        F.col("raw.DatedVehicleJourneyRef.value").alias("journey_ref"),
        F.col("raw.LineRef.value").alias("line_ref"),
        F.col("raw.PublishedLineName.value").getItem(0).alias("published_line"),
        F.col("raw.DirectionName.value").getItem(0).alias("direction"),
        F.col("raw.DestinationRef.value").alias("destination_ref"),
        F.col("raw.DestinationName.value").getItem(0).alias("destination_name"),
        F.to_timestamp(F.col("raw.RecordedAtTime")).alias("recorded_at"),
        F.col("raw.EstimatedCalls.EstimatedCall").alias("calls"),
        F.col("batch_time"),
    ).where(F.col("journey_ref").isNotNull())


def explode_calls(parsed_df):
    return (
        parsed_df.select(
            "journey_ref", "line_ref", "published_line", "direction",
            "destination_ref", "destination_name", "recorded_at", "batch_time",
            F.explode("calls").alias("call"),
        )
        .select(
            "journey_ref", "line_ref", "published_line", "direction",
            "destination_ref", "destination_name", "recorded_at", "batch_time",
            F.col("call.StopPointRef.value").alias("stop_point_ref"),
            F.to_timestamp(F.col("call.AimedArrivalTime")).alias("aimed_arrival"),
            F.to_timestamp(F.col("call.ExpectedArrivalTime")).alias("expected_arrival"),
            F.to_timestamp(F.col("call.AimedDepartureTime")).alias("aimed_departure"),
            F.to_timestamp(F.col("call.ExpectedDepartureTime")).alias("expected_departure"),
            F.col("call.ArrivalStatus").alias("arrival_status"),
            F.col("call.DepartureStatus").alias("departure_status"),
        )
        .where(F.col("stop_point_ref").isNotNull())
        .withColumn(
            "arrival_delay_sec",
            F.col("expected_arrival").cast("long") - F.col("aimed_arrival").cast("long"),
        )
        .withColumn(
            "departure_delay_sec",
            F.col("expected_departure").cast("long") - F.col("aimed_departure").cast("long"),
        )
    )


def dedup_latest_per_stop(calls_df):
    latest = Window.partitionBy("journey_ref", "stop_point_ref").orderBy(
        F.col("batch_time").desc()
    )
    return (
        calls_df.withColumn("__rn", F.row_number().over(latest))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def merge_next_stop(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {NEXT_STOP_TABLE} AS t
        USING staging_next_stop AS s
        ON t.journey_ref = s.journey_ref AND t.stop_point_ref = s.stop_point_ref
        WHEN MATCHED AND s.batch_time > t.batch_time THEN UPDATE SET
          t.line_ref            = s.line_ref,
          t.published_line      = s.published_line,
          t.direction           = s.direction,
          t.destination_ref     = s.destination_ref,
          t.destination_name    = s.destination_name,
          t.aimed_arrival       = s.aimed_arrival,
          t.expected_arrival    = s.expected_arrival,
          t.aimed_departure     = s.aimed_departure,
          t.expected_departure  = s.expected_departure,
          t.arrival_delay_sec   = s.arrival_delay_sec,
          t.departure_delay_sec = s.departure_delay_sec,
          t.arrival_status      = s.arrival_status,
          t.departure_status    = s.departure_status,
          t.recorded_at         = s.recorded_at,
          t.batch_time          = s.batch_time
        WHEN NOT MATCHED THEN INSERT (
          journey_ref, stop_point_ref, line_ref, published_line, direction,
          destination_ref, destination_name, aimed_arrival, expected_arrival,
          aimed_departure, expected_departure, arrival_delay_sec, departure_delay_sec,
          arrival_status, departure_status, recorded_at, batch_time
        ) VALUES (
          s.journey_ref, s.stop_point_ref, s.line_ref, s.published_line, s.direction,
          s.destination_ref, s.destination_name, s.aimed_arrival, s.expected_arrival,
          s.aimed_departure, s.expected_departure, s.arrival_delay_sec, s.departure_delay_sec,
          s.arrival_status, s.departure_status, s.recorded_at, s.batch_time
        )
        """
    )


def process_micro_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] empty, skipping")
        return

    spark = batch_df.sparkSession

    calls = explode_calls(batch_df)
    deduped = dedup_latest_per_stop(calls)

    deduped.createOrReplaceTempView("staging_next_stop")
    merge_next_stop(spark)

    print(f"[batch {batch_id}] merged into next_stop")


def main() -> None:
    if not CHECKPOINT_LOCATION:
        raise ValueError("CHECKPOINT_LOCATION must be set")

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)
    ensure_silver_table(spark)

    bronze_stream = (
        spark.readStream.format("iceberg")
        .option("streaming-skip-delete-snapshots", "false")
        .option("streaming-max-files-per-micro-batch", MAX_FILES_PER_MICRO_BATCH)
        .load(SOURCE_TABLE)
    )

    parsed_stream = parse_bronze(bronze_stream)

    query = (
        parsed_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
