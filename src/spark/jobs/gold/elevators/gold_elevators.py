import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

CURRENT_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.elevators_current"
HISTORY_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.elevators_history"

AVAILABILITY_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.elevators_availability"
DOWNTIME_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.elevators_downtime"
RELIABILITY_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.elevators_reliability"


def ensure_namespace(spark: SparkSession) -> None:
    # nessie_ref = spark.conf.get(f"spark.sql.catalog.{catalog_name}.ref")
    # spark.sql(
        # f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {catalog_name} FROM main"
    # )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{GOLD_NAMESPACE}"
    )


def build_availability(spark: SparkSession):
    current = spark.read.table(CURRENT_TABLE).where(F.col("is_active") == True)

    return (
        current.groupBy("station_area_id", "station_name", "transport_mode")
        .agg(
            F.count("*").alias("nb_elevators"),
            F.sum(F.col("is_available").cast("int")).alias("nb_available"),
            F.first("station_latitude", ignorenulls=True).alias("station_latitude"),
            F.first("station_longitude", ignorenulls=True).alias("station_longitude"),
        )
        .withColumn(
            "pct_available",
            F.round(100 * F.col("nb_available") / F.col("nb_elevators"), 1),
        )
        .withColumn(
            "geo_point",
            F.when(
                F.col("station_longitude").isNotNull()
                & F.col("station_latitude").isNotNull(),
                F.concat(
                    F.lit("POINT ("), F.col("station_longitude").cast("string"),
                    F.lit(" "), F.col("station_latitude").cast("string"), F.lit(")"),
                ),
            ),
        )
    )


def build_downtime(spark: SparkSession):
    history = spark.read.table(HISTORY_TABLE)

    timeline = Window.partitionBy("elevator_key").orderBy("status_updated_at")
    next_change = F.lead("status_updated_at").over(timeline)

    return (
        history.withColumn("ended_at", next_change)
        .where(F.col("is_available") == False)
        .withColumn(
            "downtime_sec",
            F.col("ended_at").cast("long") - F.col("status_updated_at").cast("long"),
        )
        .select(
            "elevator_key",
            "elevator_id",
            "station_area_id",
            "station_name",
            "transport_mode",
            "status",
            "status_reason",
            "status_updated_at",
            "ended_at",
            "downtime_sec",
        )
    )


def build_reliability(spark: SparkSession, downtime):
    now = F.current_timestamp()

    outages = downtime.withColumn(
        "downtime_minutes",
        (F.coalesce(F.col("ended_at"), now).cast("long") - F.col("status_updated_at").cast("long")) / 60,
    )

    elevator_counts = (
        spark.read.table(CURRENT_TABLE)
        .where(F.col("is_active") == True)
        .groupBy("station_area_id", "station_name", "transport_mode")
        .agg(F.count("*").alias("nb_elevators"))
    )

    def window_metrics(since_days, suffix):
        since = F.date_sub(F.current_date(), since_days)
        return (
            outages.where(F.col("status_updated_at") >= since)
            .groupBy("station_area_id", "station_name", "transport_mode")
            .agg(
                F.count("*").alias(f"nb_outages_{suffix}"),
                F.round(F.sum("downtime_minutes"), 1).alias(f"downtime_minutes_{suffix}"),
                F.round(F.avg("downtime_minutes"), 1).alias(f"avg_outage_minutes_{suffix}"),
            )
        )

    metrics_30d = window_metrics(30, "30d")
    metrics_90d = window_metrics(90, "90d")

    return (
        elevator_counts.join(metrics_30d, ["station_area_id", "station_name", "transport_mode"], "left")
        .join(metrics_90d, ["station_area_id", "station_name", "transport_mode"], "left")
        .withColumn("nb_outages_30d", F.coalesce(F.col("nb_outages_30d"), F.lit(0)))
        .withColumn("downtime_minutes_30d", F.coalesce(F.col("downtime_minutes_30d"), F.lit(0.0)))
        .withColumn("nb_outages_90d", F.coalesce(F.col("nb_outages_90d"), F.lit(0)))
        .withColumn("downtime_minutes_90d", F.coalesce(F.col("downtime_minutes_90d"), F.lit(0.0)))
        .withColumn(
            "pct_uptime_30d",
            F.round(100 * (F.lit(1) - F.least(F.col("downtime_minutes_30d") / (F.col("nb_elevators") * 30 * 1440), F.lit(1.0))), 1),
        )
        .withColumn(
            "pct_uptime_90d",
            F.round(100 * (F.lit(1) - F.least(F.col("downtime_minutes_90d") / (F.col("nb_elevators") * 90 * 1440), F.lit(1.0))), 1),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    build_availability(spark).writeTo(AVAILABILITY_TABLE).using("iceberg").createOrReplace()

    downtime = build_downtime(spark)

    (
        downtime
        .writeTo(DOWNTIME_TABLE)
        .using("iceberg")
        .partitionedBy(F.months("status_updated_at"))
        .createOrReplace()
    )

    build_reliability(spark, downtime).writeTo(RELIABILITY_TABLE).using("iceberg").createOrReplace()

    print("gold elevators tables refreshed")


if __name__ == "__main__":
    main()
