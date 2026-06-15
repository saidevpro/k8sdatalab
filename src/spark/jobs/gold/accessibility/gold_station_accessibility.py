import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

ACCESSIBILITY_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.accessibility_gares"
AVAILABILITY_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.elevators_availability"

STATION_ACCESSIBILITY_TABLE = (
    f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.station_accessibility"
)


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    # spark.sql(
        # f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    # )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def build_station_accessibility(spark: SparkSession):
    accessibility = spark.read.table(ACCESSIBILITY_TABLE).withColumn(
        "station_key", F.upper(F.trim(F.col("stop_name")))
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

    return (
        accessibility.join(availability, "station_key", "left")
        .select(
            F.col("stop_point_id"),
            F.col("stop_name"),
            F.col("accessibility_level_id"),
            F.col("stop_point_latitude"),
            F.col("stop_point_longitude"),
            F.when(
                F.col("stop_point_longitude").isNotNull()
                & F.col("stop_point_latitude").isNotNull(),
                F.concat(
                    F.lit("POINT ("), F.col("stop_point_longitude").cast("string"),
                    F.lit(" "), F.col("stop_point_latitude").cast("string"), F.lit(")"),
                ),
            ).alias("geo_point"),
            F.col("nb_elevators"),
            F.col("nb_available"),
            F.col("pct_elevators_available"),
            F.col("nb_elevators").isNotNull().alias("has_elevator_data"),
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
