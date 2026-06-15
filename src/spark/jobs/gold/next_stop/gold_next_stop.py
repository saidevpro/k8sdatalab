import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

SOURCE_TABLE = os.getenv(
    "SOURCE_TABLE", f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.next_stop"
)

SCHEDULE_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.next_stop_schedule"
DELAYS_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.next_stop_delays"
DELAYS_BY_STOP_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.delays_by_stop"

DELAY_THRESHOLD_SEC = int(os.getenv("DELAY_THRESHOLD_SEC", "60"))

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "60 seconds")
MAX_FILES_PER_MICRO_BATCH = os.getenv("MAX_FILES_PER_MICRO_BATCH", "100")


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    # spark.sql(
        # f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    # )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def ensure_gold_tables(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEDULE_TABLE} (
          journey_ref        STRING,
          stop_point_ref     STRING,
          line_ref           STRING,
          published_line     STRING,
          direction          STRING,
          destination_name   STRING,
          aimed_arrival      TIMESTAMP,
          expected_arrival   TIMESTAMP,
          aimed_departure    TIMESTAMP,
          expected_departure TIMESTAMP,
          arrival_status     STRING,
          departure_status   STRING,
          recorded_at        TIMESTAMP,
          batch_time         TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(batch_time))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DELAYS_TABLE} (
          journey_ref         STRING,
          stop_point_ref      STRING,
          line_ref            STRING,
          published_line      STRING,
          direction           STRING,
          destination_name    STRING,
          aimed_arrival       TIMESTAMP,
          expected_arrival    TIMESTAMP,
          aimed_departure     TIMESTAMP,
          expected_departure  TIMESTAMP,
          arrival_delay_sec   BIGINT,
          departure_delay_sec BIGINT,
          arrival_status      STRING,
          departure_status    STRING,
          batch_time          TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(batch_time))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DELAYS_BY_STOP_TABLE} (
          stop_point_ref        STRING,
          line_ref              STRING,
          published_line        STRING,
          nb_passages           BIGINT,
          nb_delayed            BIGINT,
          avg_arrival_delay_sec DOUBLE,
          max_arrival_delay_sec BIGINT,
          pct_on_time           DOUBLE
        )
        USING iceberg
        """
    )


def dedup_latest_per_stop(batch_df):
    latest = Window.partitionBy("journey_ref", "stop_point_ref").orderBy(
        F.col("batch_time").desc()
    )
    return (
        batch_df.withColumn("__rn", F.row_number().over(latest))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def build_delays_by_stop(spark: SparkSession):
    return (
        spark.read.table(SOURCE_TABLE)
        .where(F.col("arrival_delay_sec").isNotNull())
        .groupBy("stop_point_ref", "line_ref", "published_line")
        .agg(
            F.count("*").alias("nb_passages"),
            F.sum(
                (F.col("arrival_delay_sec") >= DELAY_THRESHOLD_SEC).cast("int")
            ).alias("nb_delayed"),
            F.round(F.avg("arrival_delay_sec"), 1).alias("avg_arrival_delay_sec"),
            F.max("arrival_delay_sec").alias("max_arrival_delay_sec"),
        )
        .withColumn(
            "pct_on_time",
            F.round(100 * (1 - F.col("nb_delayed") / F.col("nb_passages")), 1),
        )
    )


def merge_schedule(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {SCHEDULE_TABLE} AS t
        USING staging_schedule AS s
        ON t.journey_ref = s.journey_ref AND t.stop_point_ref = s.stop_point_ref
        WHEN MATCHED AND s.batch_time >= t.batch_time THEN UPDATE SET
          t.line_ref           = s.line_ref,
          t.published_line     = s.published_line,
          t.direction          = s.direction,
          t.destination_name   = s.destination_name,
          t.aimed_arrival      = s.aimed_arrival,
          t.expected_arrival   = s.expected_arrival,
          t.aimed_departure    = s.aimed_departure,
          t.expected_departure = s.expected_departure,
          t.arrival_status     = s.arrival_status,
          t.departure_status   = s.departure_status,
          t.recorded_at        = s.recorded_at,
          t.batch_time         = s.batch_time
        WHEN NOT MATCHED THEN INSERT (
          journey_ref, stop_point_ref, line_ref, published_line, direction,
          destination_name, aimed_arrival, expected_arrival, aimed_departure,
          expected_departure, arrival_status, departure_status, recorded_at, batch_time
        ) VALUES (
          s.journey_ref, s.stop_point_ref, s.line_ref, s.published_line, s.direction,
          s.destination_name, s.aimed_arrival, s.expected_arrival, s.aimed_departure,
          s.expected_departure, s.arrival_status, s.departure_status, s.recorded_at, s.batch_time
        )
        """
    )


def merge_delays(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {DELAYS_TABLE} AS t
        USING staging_delays AS s
        ON t.journey_ref = s.journey_ref AND t.stop_point_ref = s.stop_point_ref
           AND t.batch_time = s.batch_time
        WHEN NOT MATCHED THEN INSERT (
          journey_ref, stop_point_ref, line_ref, published_line, direction,
          destination_name, aimed_arrival, expected_arrival, aimed_departure,
          expected_departure, arrival_delay_sec, departure_delay_sec,
          arrival_status, departure_status, batch_time
        ) VALUES (
          s.journey_ref, s.stop_point_ref, s.line_ref, s.published_line, s.direction,
          s.destination_name, s.aimed_arrival, s.expected_arrival, s.aimed_departure,
          s.expected_departure, s.arrival_delay_sec, s.departure_delay_sec,
          s.arrival_status, s.departure_status, s.batch_time
        )
        """
    )


def merge_delays_by_stop(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {DELAYS_BY_STOP_TABLE} AS t
        USING staging_delays_by_stop AS s
        ON t.stop_point_ref = s.stop_point_ref AND t.line_ref <=> s.line_ref
           AND t.published_line <=> s.published_line
        WHEN MATCHED THEN UPDATE SET
          t.nb_passages           = s.nb_passages,
          t.nb_delayed            = s.nb_delayed,
          t.avg_arrival_delay_sec = s.avg_arrival_delay_sec,
          t.max_arrival_delay_sec = s.max_arrival_delay_sec,
          t.pct_on_time           = s.pct_on_time
        WHEN NOT MATCHED THEN INSERT (
          stop_point_ref, line_ref, published_line, nb_passages, nb_delayed,
          avg_arrival_delay_sec, max_arrival_delay_sec, pct_on_time
        ) VALUES (
          s.stop_point_ref, s.line_ref, s.published_line, s.nb_passages, s.nb_delayed,
          s.avg_arrival_delay_sec, s.max_arrival_delay_sec, s.pct_on_time
        )
        """
    )


def process_micro_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] empty, skipping")
        return

    spark = batch_df.sparkSession

    deduped = dedup_latest_per_stop(batch_df).cache()

    deduped.select(
        "journey_ref", "stop_point_ref", "line_ref", "published_line", "direction",
        "destination_name", "aimed_arrival", "expected_arrival", "aimed_departure",
        "expected_departure", "arrival_status", "departure_status", "recorded_at",
        "batch_time",
    ).createOrReplaceTempView("staging_schedule")

    (
        deduped.where(
            (F.col("arrival_delay_sec") >= DELAY_THRESHOLD_SEC)
            | (F.col("departure_delay_sec") >= DELAY_THRESHOLD_SEC)
        )
        .select(
            "journey_ref", "stop_point_ref", "line_ref", "published_line", "direction",
            "destination_name", "aimed_arrival", "expected_arrival", "aimed_departure",
            "expected_departure", "arrival_delay_sec", "departure_delay_sec",
            "arrival_status", "departure_status", "batch_time",
        )
        .createOrReplaceTempView("staging_delays")
    )

    build_delays_by_stop(spark).createOrReplaceTempView("staging_delays_by_stop")

    merge_schedule(spark)
    merge_delays(spark)
    merge_delays_by_stop(spark)

    deduped.unpersist()
    print(f"[batch {batch_id}] merged into schedule/delays/delays_by_stop")


def main() -> None:
    if not CHECKPOINT_LOCATION:
        raise ValueError("CHECKPOINT_LOCATION must be set")

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)
    ensure_gold_tables(spark)

    silver_stream = (
        spark.readStream.format("iceberg")
        .option("streaming-skip-delete-snapshots", "false")
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-max-files-per-micro-batch", MAX_FILES_PER_MICRO_BATCH)
        .load(SOURCE_TABLE)
    )

    query = (
        silver_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
