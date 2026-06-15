import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window


catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", f"{catalog_name}.bronze.disruptions")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")

MESSAGES_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.disruption_messages"
STOPS_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.disruption_stops"
PERIODS_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.disruption_periods"

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "60 seconds")
MAX_FILES_PER_MICRO_BATCH = os.getenv("MAX_FILES_PER_MICRO_BATCH", "100")


POINT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

APPLICATION_PERIOD_SCHEMA = StructType(
    [
        StructField("begin", StringType(), True),
        StructField("end", StringType(), True),
    ]
)

IMPACTED_SECTION_SCHEMA = StructType(
    [
        StructField("from", POINT_SCHEMA, True),
        StructField("lineId", StringType(), True),
        StructField("to", POINT_SCHEMA, True),
    ]
)

RAW_JSON_SCHEMA = StructType(
    [
        StructField("applicationPeriods", ArrayType(APPLICATION_PERIOD_SCHEMA), True),
        StructField("cause", StringType(), True),
        StructField("id", StringType(), True),
        StructField("impactedSections", ArrayType(IMPACTED_SECTION_SCHEMA), True),
        StructField("lastUpdate", StringType(), True),
        StructField("message", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("shortMessage", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("title", StringType(), True),
    ]
)


def ensure_namespace(spark: SparkSession) -> None:
    # nessie_ref = spark.conf.get(f"spark.sql.catalog.{catalog_name}.ref")
    # spark.sql(
        # f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {catalog_name} FROM main"
    # )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{SILVER_NAMESPACE}"
    )


def ensure_silver_tables(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {MESSAGES_TABLE} (
          id            STRING,
          cause         STRING,
          severity      STRING,
          title         STRING,
          short_message STRING,
          message       STRING,
          last_update   TIMESTAMP,
          batch_time    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(batch_time))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {STOPS_TABLE} (
          id         STRING,
          line_id    STRING,
          from_id    STRING,
          from_name  STRING,
          to_id      STRING,
          to_name    STRING,
          title      STRING,
          severity   STRING,
          batch_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(batch_time))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {PERIODS_TABLE} (
          id         STRING,
          title      STRING,
          severity   STRING,
          begin_time TIMESTAMP,
          end_time   TIMESTAMP,
          batch_time TIMESTAMP
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
        F.col("raw.id").alias("id"),
        F.col("raw.cause").alias("cause"),
        F.col("raw.severity").alias("severity"),
        F.col("raw.title").alias("title"),
        F.col("raw.shortMessage").alias("short_message"),
        F.col("raw.message").alias("message"),
        F.to_timestamp(F.col("raw.lastUpdate"), "yyyyMMdd'T'HHmmss").alias(
            "last_update"
        ),
        F.col("raw.applicationPeriods").alias("application_periods"),
        F.col("raw.impactedSections").alias("impacted_sections"),
        F.col("batch_time"),
    ).where(F.col("id").isNotNull())


def dedup_latest_per_id(batch_df):
    latest = Window.partitionBy("id").orderBy(F.col("batch_time").desc())
    return (
        batch_df.withColumn("__rn", F.row_number().over(latest))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def merge_messages(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {MESSAGES_TABLE} AS t
        USING staging_messages AS s
        ON t.id = s.id
        WHEN MATCHED AND s.batch_time > t.batch_time THEN UPDATE SET
          t.cause         = s.cause,
          t.severity      = s.severity,
          t.title         = s.title,
          t.short_message = s.short_message,
          t.message       = s.message,
          t.last_update   = s.last_update,
          t.batch_time    = s.batch_time
        WHEN NOT MATCHED THEN INSERT (
          id, cause, severity, title, short_message, message, last_update, batch_time
        ) VALUES (
          s.id, s.cause, s.severity, s.title, s.short_message, s.message,
          s.last_update, s.batch_time
        )
        """
    )


def merge_stops(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {STOPS_TABLE} AS t
        USING staging_stops AS s
        ON t.id = s.id AND t.line_id <=> s.line_id
           AND t.from_id <=> s.from_id AND t.to_id <=> s.to_id
        WHEN MATCHED AND s.batch_time > t.batch_time THEN UPDATE SET
          t.from_name  = s.from_name,
          t.to_name    = s.to_name,
          t.title      = s.title,
          t.severity   = s.severity,
          t.batch_time = s.batch_time
        WHEN NOT MATCHED THEN INSERT (
          id, line_id, from_id, from_name, to_id, to_name, title, severity, batch_time
        ) VALUES (
          s.id, s.line_id, s.from_id, s.from_name, s.to_id, s.to_name,
          s.title, s.severity, s.batch_time
        )
        """
    )


def merge_periods(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {PERIODS_TABLE} AS t
        USING staging_periods AS s
        ON t.id = s.id AND t.begin_time <=> s.begin_time AND t.end_time <=> s.end_time
        WHEN MATCHED AND s.batch_time > t.batch_time THEN UPDATE SET
          t.title      = s.title,
          t.severity   = s.severity,
          t.batch_time = s.batch_time
        WHEN NOT MATCHED THEN INSERT (
          id, title, severity, begin_time, end_time, batch_time
        ) VALUES (
          s.id, s.title, s.severity, s.begin_time, s.end_time, s.batch_time
        )
        """
    )


def process_micro_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] empty, skipping")
        return

    spark = batch_df.sparkSession

    deduped = dedup_latest_per_id(batch_df).cache()

    messages_df = deduped.select(
        "id", "cause", "severity", "title", "short_message", "message",
        "last_update", "batch_time"
    )

    stops_df = (
        deduped
        .select(
            "id", "title", "severity", "batch_time",
            F.explode_outer("impacted_sections").alias("section"),
        )
        .select(
            "id", "title", "severity", "batch_time",
            F.substring_index(F.col("section.lineId"), ":", -1).alias("line_id"),
            F.substring_index(F.col("section.from.id"), ":", -1).alias("from_id"),
            F.col("section.from.name").alias("from_name"),
            F.substring_index(F.col("section.to.id"), ":", -1).alias("to_id"),
            F.col("section.to.name").alias("to_name"),
        )
        .where(F.col("line_id").isNotNull())
    )

    periods_df = (
        deduped
        .select(
            "id", "title", "severity", "batch_time",
            F.explode_outer("application_periods").alias("period"),
        )
        .select(
            "id", "title", "severity", "batch_time",
            F.to_timestamp(F.col("period.begin"), "yyyyMMdd'T'HHmmss").alias("begin_time"),
            F.to_timestamp(F.col("period.end"), "yyyyMMdd'T'HHmmss").alias("end_time"),
        )
        .where(F.col("begin_time").isNotNull())
    )

    messages_df.createOrReplaceTempView("staging_messages")
    stops_df.createOrReplaceTempView("staging_stops")
    periods_df.createOrReplaceTempView("staging_periods")

    merge_messages(spark)
    merge_stops(spark)
    merge_periods(spark)

    deduped.unpersist()
    print(f"[batch {batch_id}] merged into messages/stops/periods")


def main() -> None:
    if not CHECKPOINT_LOCATION:
        raise ValueError("CHECKPOINT_LOCATION must be set")

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)
    ensure_silver_tables(spark)

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
