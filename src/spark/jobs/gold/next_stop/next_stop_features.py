import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

NEXT_STOP_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.next_stop"
CALENDAR_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.day_type_calendar"
DELAYS_BY_STOP_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.delays_by_stop"

NEXT_STOP_FEATURES_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.next_stop_features"

DELAY_THRESHOLD_SEC = int(os.getenv("DELAY_THRESHOLD_SEC", "60"))


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    spark.sql(
        f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def build_next_stop_features(spark: SparkSession):
    next_stop = spark.read.table(NEXT_STOP_TABLE).where(
        F.col("arrival_delay_sec").isNotNull() & F.col("aimed_arrival").isNotNull()
    )
    calendar = spark.read.table(CALENDAR_TABLE)
    delays_by_stop = spark.read.table(DELAYS_BY_STOP_TABLE).select(
        F.col("stop_point_ref"),
        F.col("line_ref"),
        F.col("published_line"),
        F.col("avg_arrival_delay_sec").alias("hist_avg_arrival_delay_sec"),
        F.col("pct_on_time").alias("hist_pct_on_time"),
    )

    return (
        next_stop.alias("n")
        .withColumn("service_date", F.to_date(F.col("aimed_arrival")))
        .join(calendar.alias("c"), F.col("service_date") == F.col("c.date"), "left")
        .join(
            delays_by_stop.alias("h"),
            (F.col("n.stop_point_ref") == F.col("h.stop_point_ref"))
            & (F.col("n.line_ref").eqNullSafe(F.col("h.line_ref")))
            & (F.col("n.published_line").eqNullSafe(F.col("h.published_line"))),
            "left",
        )
        .select(
            F.col("n.journey_ref").alias("journey_ref"),
            F.col("n.stop_point_ref").alias("stop_point_ref"),
            F.col("n.line_ref").alias("line_ref"),
            F.col("n.published_line").alias("published_line"),
            F.col("n.direction").alias("direction"),
            F.col("service_date").alias("service_date"),
            F.hour("n.aimed_arrival").alias("hour"),
            F.col("c.day_of_week").alias("day_of_week"),
            F.col("c.is_weekend").alias("is_weekend"),
            F.col("c.is_holiday").alias("is_holiday"),
            F.col("c.is_school_holiday").alias("is_school_holiday"),
            F.col("c.cat_jour").alias("cat_jour"),
            F.col("h.hist_avg_arrival_delay_sec").alias("hist_avg_arrival_delay_sec"),
            F.col("h.hist_pct_on_time").alias("hist_pct_on_time"),
            F.col("n.arrival_delay_sec").alias("arrival_delay_sec"),
            (F.col("n.arrival_delay_sec") >= DELAY_THRESHOLD_SEC).alias("is_delayed"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    (
        build_next_stop_features(spark)
        .writeTo(NEXT_STOP_FEATURES_TABLE)
        .using("iceberg")
        .partitionedBy(F.months("service_date"))
        .createOrReplace()
    )

    print("gold next_stop_features refreshed")


if __name__ == "__main__":
    main()
