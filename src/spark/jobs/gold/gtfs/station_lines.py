import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

TRIP_SCHEDULE_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.trip_schedule"
DIM_STOPS_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.dim_stops"

STATION_LINES_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.station_lines"


def ensure_namespace(spark: SparkSession) -> None:
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{GOLD_NAMESPACE}"
    )


def build_station_lines(spark: SparkSession):
    trip_schedule = spark.read.table(TRIP_SCHEDULE_TABLE)
    dim_stops = spark.read.table(DIM_STOPS_TABLE).select(
        "stop_id", "parent_station_id", "parent_station_name"
    )

    return (    
        trip_schedule
        .select(
            "stop_id", "route_id", "route_short_name",
            "route_long_name", "route_type", "agency_id",
        )
        .distinct()
        .join(dim_stops, "stop_id", "left")
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    build_station_lines(spark).writeTo(STATION_LINES_TABLE).using("iceberg").createOrReplace()

    print("gold station_lines refreshed")


if __name__ == "__main__":
    main()
