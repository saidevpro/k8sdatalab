import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

STOP_POINTS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.stop_points"
STOP_AREAS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.stop_areas"
ACCESSIBILITY_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.accessibility_gares"
WHEELCHAIRS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.wheelchairs"

DIM_STOPS_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.dim_stops"


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    spark.sql(
        f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def wkt_point(lon, lat):
    return F.when(
        lon.isNotNull() & lat.isNotNull(),
        F.concat(
            F.lit("POINT ("), lon.cast("string"),
            F.lit(" "), lat.cast("string"), F.lit(")"),
        ),
    )


def build_dim_stops(spark: SparkSession):
    stop_points = spark.read.table(STOP_POINTS_TABLE)
    stop_areas = spark.read.table(STOP_AREAS_TABLE)
    accessibility = spark.read.table(ACCESSIBILITY_TABLE).withColumn(
        "stop_id", F.regexp_extract(F.col("stop_point_id"), r"(\d+)", 1).cast("int")
    )
    wheelchairs = spark.read.table(WHEELCHAIRS_TABLE).select(
        "stop_id", "wheelchair_boarding"
    )

    return (
        stop_points.alias("sp")
        .join(
            stop_areas.alias("sa"),
            F.col("sp.parent_station") == F.col("sa.station_id"),
            "left",
        )
        .join(
            accessibility.alias("ac").select("stop_id", "accessibility_level_id"),
            "stop_id",
            "left",
        )
        .join(wheelchairs.alias("wc"), "stop_id", "left")
        .select(
            F.col("sp.stop_id").alias("stop_id"),
            F.col("sp.stop_name").alias("stop_name"),
            F.col("sp.zone_id").alias("zone_id"),
            F.col("sp.stop_lon").alias("stop_lon"),
            F.col("sp.stop_lat").alias("stop_lat"),
            wkt_point(F.col("sp.stop_lon"), F.col("sp.stop_lat")).alias("stop_geo_point"),
            F.col("sa.station_id").alias("parent_station_id"),
            F.col("sa.station_name").alias("parent_station_name"),
            F.col("sa.station_lon").alias("parent_station_lon"),
            F.col("sa.station_lat").alias("parent_station_lat"),
            wkt_point(F.col("sa.station_lon"), F.col("sa.station_lat")).alias("parent_station_geo_point"),
            F.col("ac.accessibility_level_id").alias("accessibility_level_id"),
            F.col("wc.wheelchair_boarding").alias("wheelchair_boarding"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    build_dim_stops(spark).writeTo(DIM_STOPS_TABLE).using("iceberg").createOrReplace()

    print("gold dim_stops refreshed")


if __name__ == "__main__":
    main()
