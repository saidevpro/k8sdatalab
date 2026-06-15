import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

ROUTES_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.routes"
TRIPS_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.trips"
STOP_TIMES_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.stop_times"
STOP_POINTS_TABLE = f"{catalog_name}.{SILVER_NAMESPACE}.stop_points"

TRIP_SCHEDULE_TABLE = f"{catalog_name}.{GOLD_NAMESPACE}.trip_schedule"


def ensure_namespace(spark: SparkSession) -> None:
    # nessie_ref = spark.conf.get(f"spark.sql.catalog.{catalog_name}.ref")
    # spark.sql(
        # f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {catalog_name} FROM main"
    # )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{GOLD_NAMESPACE}"
    )


def build_trip_schedule(spark: SparkSession):
    routes = spark.read.table(ROUTES_TABLE)
    trips = spark.read.table(TRIPS_TABLE)
    stop_times = spark.read.table(STOP_TIMES_TABLE)
    stop_points = spark.read.table(STOP_POINTS_TABLE).select("stop_id", "stop_name")

    return (
        stop_times.alias("st")
        .join(trips.alias("tr"), "trip_id", "inner")
        .join(routes.alias("rt"), "route_id", "left")
        .join(stop_points.alias("sp"), "stop_id", "left")
        .select(
            F.col("rt.route_id").alias("route_id"),
            F.col("rt.route_short_name").alias("route_short_name"),
            F.col("rt.route_long_name").alias("route_long_name"),
            F.col("rt.route_type").alias("route_type"),
            F.col("rt.agency_id").alias("agency_id"),
            F.col("tr.service_id").alias("service_id"),
            F.col("tr.trip_id").alias("trip_id"),
            F.col("tr.trip_headsign").alias("trip_headsign"),
            F.col("tr.direction_id").alias("direction_id"),
            F.col("st.stop_id").alias("stop_id"),
            F.col("sp.stop_name").alias("stop_name"),
            F.col("st.stop_sequence").alias("stop_sequence"),
            F.col("st.arrival_time").alias("arrival_time"),
            F.col("st.departure_time").alias("departure_time"),
            F.col("st.arrival_seconds").alias("arrival_seconds"),
            F.col("st.departure_seconds").alias("departure_seconds"),
            F.col("st.pickup_type").alias("pickup_type"),
            F.col("st.drop_off_type").alias("drop_off_type"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    (
        build_trip_schedule(spark)
        .writeTo(TRIP_SCHEDULE_TABLE)
        .using("iceberg")
        .partitionedBy(F.col("route_type"))
        .createOrReplace()
    )

    print("gold trip_schedule refreshed")


if __name__ == "__main__":
    main()
