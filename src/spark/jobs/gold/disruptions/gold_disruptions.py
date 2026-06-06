import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

SOURCE_TABLE = os.getenv(
    "SOURCE_TABLE", f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.disruption_messages"
)
PERIODS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.disruption_periods"
STOPS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.disruption_stops"

ACTIVE_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.disruptions_active"
BY_LINE_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.disruptions_by_line"

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "60 seconds")
MAX_FILES_PER_MICRO_BATCH = os.getenv("MAX_FILES_PER_MICRO_BATCH", "100")


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    spark.sql(
        f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def ensure_gold_tables(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ACTIVE_TABLE} (
          id             STRING,
          cause          STRING,
          severity       STRING,
          title          STRING,
          short_message  STRING,
          message        STRING,
          begin_time     TIMESTAMP,
          end_time       TIMESTAMP,
          last_update    TIMESTAMP,
          impacted_lines ARRAY<STRING>,
          is_active      BOOLEAN,
          batch_time     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(batch_time))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BY_LINE_TABLE} (
          line_id        STRING,
          day            DATE,
          nb_disruptions BIGINT,
          severities     ARRAY<STRING>
        )
        USING iceberg
        PARTITIONED BY (months(day))
        """
    )


def dedup_latest_per_id(messages_df):
    latest = Window.partitionBy("id").orderBy(F.col("batch_time").desc())
    return (
        messages_df.withColumn("__rn", F.row_number().over(latest))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def build_active(messages_df, periods_df, stops_df):
    now = F.current_timestamp()

    periods_agg = periods_df.groupBy("id").agg(
        F.min("begin_time").alias("begin_time"),
        F.max("end_time").alias("end_time"),
        F.max(
            ((F.col("begin_time") <= now) & (F.col("end_time") >= now)).cast("int")
        ).alias("active_flag"),
    )

    impacted = stops_df.groupBy("id").agg(
        F.collect_set("line_id").alias("impacted_lines")
    )

    return (
        messages_df.alias("m")
        .join(periods_agg.alias("p"), "id", "left")
        .join(impacted.alias("s"), "id", "left")
        .select(
            F.col("m.id").alias("id"),
            F.col("m.cause").alias("cause"),
            F.col("m.severity").alias("severity"),
            F.col("m.title").alias("title"),
            F.col("m.short_message").alias("short_message"),
            F.col("m.message").alias("message"),
            F.col("p.begin_time").alias("begin_time"),
            F.col("p.end_time").alias("end_time"),
            F.col("m.last_update").alias("last_update"),
            F.col("s.impacted_lines").alias("impacted_lines"),
            (F.col("p.active_flag") == 1).alias("is_active"),
            F.col("m.batch_time").alias("batch_time"),
        )
    )


def build_by_line(stops_df):
    return (
        stops_df.where(F.col("line_id").isNotNull())
        .withColumn("day", F.to_date(F.col("batch_time")))
        .groupBy("line_id", "day")
        .agg(
            F.countDistinct("id").alias("nb_disruptions"),
            F.collect_set("severity").alias("severities"),
        )
    )


def merge_active(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {ACTIVE_TABLE} AS t
        USING staging_active AS s
        ON t.id = s.id
        WHEN MATCHED AND s.batch_time >= t.batch_time THEN UPDATE SET
          t.cause          = s.cause,
          t.severity       = s.severity,
          t.title          = s.title,
          t.short_message  = s.short_message,
          t.message        = s.message,
          t.begin_time     = s.begin_time,
          t.end_time       = s.end_time,
          t.last_update    = s.last_update,
          t.impacted_lines = s.impacted_lines,
          t.is_active      = s.is_active,
          t.batch_time     = s.batch_time
        WHEN NOT MATCHED THEN INSERT (
          id, cause, severity, title, short_message, message, begin_time,
          end_time, last_update, impacted_lines, is_active, batch_time
        ) VALUES (
          s.id, s.cause, s.severity, s.title, s.short_message, s.message, s.begin_time,
          s.end_time, s.last_update, s.impacted_lines, s.is_active, s.batch_time
        )
        """
    )


def merge_by_line(spark: SparkSession) -> None:
    spark.sql(
        f"""
        MERGE INTO {BY_LINE_TABLE} AS t
        USING staging_by_line AS s
        ON t.line_id = s.line_id AND t.day = s.day
        WHEN MATCHED THEN UPDATE SET
          t.nb_disruptions = s.nb_disruptions,
          t.severities     = s.severities
        WHEN NOT MATCHED THEN INSERT (
          line_id, day, nb_disruptions, severities
        ) VALUES (
          s.line_id, s.day, s.nb_disruptions, s.severities
        )
        """
    )


def process_micro_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] empty, skipping")
        return

    spark = batch_df.sparkSession

    periods_df = spark.read.table(PERIODS_TABLE)
    stops_df = spark.read.table(STOPS_TABLE)

    messages_df = dedup_latest_per_id(batch_df)

    build_active(messages_df, periods_df, stops_df).createOrReplaceTempView(
        "staging_active"
    )
    build_by_line(stops_df).createOrReplaceTempView("staging_by_line")

    merge_active(spark)
    merge_by_line(spark)

    print(f"[batch {batch_id}] merged into disruptions_active/by_line")


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
