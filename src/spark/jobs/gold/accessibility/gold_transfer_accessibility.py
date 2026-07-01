import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

TRANSFER_WALKING_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.transfer_walking"
DIM_STOPS_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.dim_stops"
AVAILABILITY_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.elevators_availability"
RELIABILITY_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.elevators_reliability"

TRANSFER_ACCESSIBILITY_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.transfer_accessibility"


def ensure_namespace(spark: SparkSession) -> None:
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{GOLD_NAMESPACE}"
    )


def build_transfer_accessibility(spark: SparkSession):
    transfers = spark.read.table(TRANSFER_WALKING_TABLE)

    stops = spark.read.table(DIM_STOPS_TABLE).select(
        F.col("stop_id"),
        F.upper(F.trim(F.col("parent_station_name"))).alias("station_key"),
    )

    availability = (
        spark.read.table(AVAILABILITY_TABLE)
        .withColumn("station_key", F.upper(F.trim(F.col("station_name"))))
        .groupBy("station_key")
        .agg(
            F.sum("nb_elevators").alias("nb_elevators"),
            F.sum("nb_available").alias("nb_available"),
        )
    )

    reliability = (
        spark.read.table(RELIABILITY_TABLE)
        .withColumn("station_key", F.upper(F.trim(F.col("station_name"))))
        .groupBy("station_key")
        .agg(F.round(F.avg("pct_uptime_30d"), 1).alias("pct_uptime_30d"))
    )

    from_stations = stops.select(
        F.col("stop_id").alias("from_stop_id"),
        F.col("station_key").alias("from_station_key"),
    )

    return (
        transfers.alias("t")
        .join(from_stations, "from_stop_id", "left")
        .join(availability.alias("a"), F.col("from_station_key") == F.col("a.station_key"), "left")
        .join(reliability.alias("r"), F.col("from_station_key") == F.col("r.station_key"), "left")
        .select(
            F.col("t.from_stop_id"),
            F.col("t.to_stop_id"),
            F.col("t.transfer_type"),
            F.col("t.min_transfer_time"),
            F.col("t.pathway_length_m"),
            F.col("t.pathway_traversal_time"),
            F.col("t.has_stairs"),
            F.col("t.has_escalator"),
            F.col("t.has_elevator"),
            F.col("a.nb_elevators"),
            F.col("a.nb_available"),
            F.col("r.pct_uptime_30d"),
            (
                ~F.col("t.has_stairs")
                | (F.col("t.has_elevator") & (F.coalesce(F.col("a.nb_available"), F.lit(0)) > 0))
            ).alias("accessible_transfer"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    (
        build_transfer_accessibility(spark)
        .writeTo(TRANSFER_ACCESSIBILITY_TABLE)
        .using("iceberg")
        .createOrReplace()
    )

    print("gold transfer_accessibility refreshed")


if __name__ == "__main__":
    main()
