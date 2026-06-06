import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SOURCE_TABLE = os.getenv("SOURCE_TABLE", "nessie.silver.next_stop")
NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

SCHEDULE_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.next_stop_schedule"
DELAYS_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.next_stop_delays"
DELAYS_BY_STOP_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.delays_by_stop"

DELAY_THRESHOLD_SEC = int(os.getenv("DELAY_THRESHOLD_SEC", "60"))


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    spark.sql(
        f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def build_schedule(silver_df):
    now = F.current_timestamp()
    return (
        silver_df.where(
            (F.col("expected_arrival") >= now)
            | (F.col("expected_departure") >= now)
        )
        .select(
            "journey_ref", "stop_point_ref", "line_ref", "published_line",
            "direction", "destination_name", "aimed_arrival", "expected_arrival",
            "aimed_departure", "expected_departure", "arrival_status",
            "departure_status", "recorded_at",
        )
        .orderBy("published_line", "expected_arrival")
    )


def build_delays(silver_df):
    return (
        silver_df.where(
            (F.col("arrival_delay_sec") >= DELAY_THRESHOLD_SEC)
            | (F.col("departure_delay_sec") >= DELAY_THRESHOLD_SEC)
        )
        .select(
            "journey_ref", "stop_point_ref", "line_ref", "published_line",
            "direction", "destination_name", "aimed_arrival", "expected_arrival",
            "aimed_departure", "expected_departure", "arrival_delay_sec",
            "departure_delay_sec", "arrival_status", "departure_status",
            "batch_time",
        )
    )


def build_delays_by_stop(silver_df):
    return (
        silver_df.where(F.col("arrival_delay_sec").isNotNull())
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
            F.round(
                100 * (1 - F.col("nb_delayed") / F.col("nb_passages")), 1
            ),
        )
    )


def overwrite_table(df, table, partition_col=None):
    writer = df.writeTo(table).using("iceberg")
    if partition_col:
        writer = writer.partitionedBy(F.days(partition_col))
    writer.createOrReplace()


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    silver_df = spark.read.table(SOURCE_TABLE).cache()

    overwrite_table(build_schedule(silver_df), SCHEDULE_TABLE)
    overwrite_table(build_delays(silver_df), DELAYS_TABLE, "batch_time")
    overwrite_table(build_delays_by_stop(silver_df), DELAYS_BY_STOP_TABLE)

    silver_df.unpersist()
    print("gold next_stop tables refreshed")


if __name__ == "__main__":
    main()
