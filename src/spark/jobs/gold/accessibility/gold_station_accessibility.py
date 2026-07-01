import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

DIM_STOPS_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.dim_stops"
AVAILABILITY_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.elevators_availability"
RELIABILITY_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.elevators_reliability"
STATION_LINES_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.station_lines"
DISRUPTIONS_BY_LINE_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.disruptions_by_line"
CROWDING_FEATURES_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.crowding_features"

STATION_ACCESSIBILITY_TABLE = (
    f"{catalog_name}.{GOLD_NAMESPACE}.station_accessibility"
)


def ensure_namespace(spark: SparkSession) -> None:
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{GOLD_NAMESPACE}"
    )


def build_station_accessibility(spark: SparkSession):
    dim_stops = spark.read.table(DIM_STOPS_TABLE).withColumn(
        "station_key", F.upper(F.trim(F.col("parent_station_name")))
    )

    availability = (
        spark.read.table(AVAILABILITY_TABLE)
        .withColumn("station_key", F.upper(F.trim(F.col("station_name"))))
        .groupBy("station_key")
        .agg(
            F.sum("nb_elevators").alias("nb_elevators"),
            F.sum("nb_available").alias("nb_available"),
        )
        .withColumn(
            "pct_elevators_available",
            F.round(100 * F.col("nb_available") / F.col("nb_elevators"), 1),
        )
    )

    reliability = (
        spark.read.table(RELIABILITY_TABLE)
        .withColumn("station_key", F.upper(F.trim(F.col("station_name"))))
        .groupBy("station_key")
        .agg(
            F.sum("nb_outages_30d").alias("nb_outages_30d"),
            F.round(F.avg("pct_uptime_30d"), 1).alias("pct_uptime_30d"),
        )
    )

    station_lines = spark.read.table(STATION_LINES_TABLE).select(
        "stop_id", "route_id", "route_short_name", "route_type"
    )

    lines_summary = station_lines.groupBy("stop_id").agg(
        F.collect_set("route_short_name").alias("lines_served"),
        F.collect_set("route_type").alias("modes_served"),
        F.countDistinct("route_id").alias("nb_lines_served"),
    )

    disruptions_today = (
        spark.read.table(DISRUPTIONS_BY_LINE_TABLE)
        .where(F.col("day") == F.current_date())
        .groupBy("line_id")
        .agg(F.sum("nb_disruptions").alias("nb_disruptions"))
    )

    disruptions_by_stop = (
        station_lines.alias("l")
        .join(disruptions_today.alias("d"), F.col("l.route_id") == F.col("d.line_id"), "inner")
        .groupBy("stop_id")
        .agg(F.sum("nb_disruptions").alias("nb_active_disruptions_today"))
    )

    crowding = (
        spark.read.table(CROWDING_FEATURES_TABLE)
        .where(F.col("jour") >= F.date_sub(F.current_date(), 30))
        .groupBy("id_refa_lda")
        .agg(
            F.round(F.avg("daily_total_validations"), 0).alias("avg_daily_validations_30d"),
            F.round(F.max("pct_validations"), 1).alias("peak_hour_pct_validations"),
        )
    )

    return (
        dim_stops.alias("d")
        .join(availability.alias("a"), "station_key", "left")
        .join(reliability.alias("r"), "station_key", "left")
        .join(lines_summary.alias("l"), F.col("d.stop_id") == F.col("l.stop_id"), "left")
        .join(disruptions_by_stop.alias("dis"), F.col("d.stop_id") == F.col("dis.stop_id"), "left")
        .join(crowding.alias("c"), F.col("d.parent_station_id") == F.col("c.id_refa_lda"), "left")
        .select(
            F.col("d.stop_id"),
            F.col("d.stop_name"),
            F.col("d.parent_station_id"),
            F.col("d.parent_station_name"),
            F.col("d.stop_geo_point"),
            F.col("d.accessibility_level_id"),
            F.col("d.wheelchair_boarding"),
            F.col("a.nb_elevators"),
            F.col("a.nb_available"),
            F.col("a.pct_elevators_available"),
            F.col("a.nb_elevators").isNotNull().alias("has_elevator_data"),
            F.col("r.nb_outages_30d"),
            F.col("r.pct_uptime_30d"),
            F.coalesce(F.col("l.lines_served"), F.array()).alias("lines_served"),
            F.coalesce(F.col("l.modes_served"), F.array()).alias("modes_served"),
            F.coalesce(F.col("l.nb_lines_served"), F.lit(0)).alias("nb_lines_served"),
            F.coalesce(F.col("dis.nb_active_disruptions_today"), F.lit(0)).alias("nb_active_disruptions_today"),
            F.col("c.avg_daily_validations_30d"),
            F.col("c.peak_hour_pct_validations"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    (
        build_station_accessibility(spark)
        .writeTo(STATION_ACCESSIBILITY_TABLE)
        .using("iceberg")
        .createOrReplace()
    )

    print("gold station_accessibility refreshed")


if __name__ == "__main__":
    main()
