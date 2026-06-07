import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

DAILY_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.validations_daily"
CALENDAR_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.day_type_calendar"
STOP_AREAS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.stop_areas"

DAILY_BY_STOP_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.validations_daily_by_stop"
BY_CATEGORY_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.validations_by_category"


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    spark.sql(
        f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def load_geo(spark: SparkSession):
    return spark.read.table(STOP_AREAS_TABLE).select(
        F.col("station_id").alias("id_refa_lda"),
        F.when(
            F.col("station_lon").isNotNull() & F.col("station_lat").isNotNull(),
            F.concat(
                F.lit("POINT ("), F.col("station_lon").cast("string"),
                F.lit(" "), F.col("station_lat").cast("string"), F.lit(")"),
            ),
        ).alias("geo_point"),
    )


def build_daily_by_stop(spark: SparkSession):
    daily = spark.read.table(DAILY_TABLE)
    calendar = spark.read.table(CALENDAR_TABLE)
    geo = load_geo(spark)

    return (
        daily.groupBy(
            "jour", "stif_trns", "stif_res", "stif_arret", "id_refa_lda", "libelle_arret"
        )
        .agg(F.sum("nb_vald").alias("total_validations"))
        .join(calendar, F.col("jour") == F.col("date"), "left")
        .join(geo, "id_refa_lda", "left")
        .withColumn("month", F.month("jour"))
        .withColumn("year", F.year("jour"))
        .select(
            "jour", "year", "month", "day_of_week", "is_weekend", "is_holiday",
            "cat_jour", "stif_trns", "stif_res", "stif_arret", "id_refa_lda",
            "libelle_arret", "total_validations", "geo_point",
        )
    )


def build_by_category(spark: SparkSession):
    daily = spark.read.table(DAILY_TABLE)
    calendar = spark.read.table(CALENDAR_TABLE)

    return (
        daily.join(calendar, F.col("jour") == F.col("date"), "left")
        .withColumn("month", F.month("jour"))
        .withColumn("year", F.year("jour"))
        .select(
            "jour", "year", "month", "day_of_week", "is_weekend", "cat_jour",
            "stif_trns", "stif_res", "stif_arret", "id_refa_lda", "libelle_arret",
            "categorie_titre", "nb_vald",
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    (
        build_daily_by_stop(spark)
        .writeTo(DAILY_BY_STOP_TABLE)
        .using("iceberg")
        .partitionedBy(F.months("jour"))
        .createOrReplace()
    )

    (
        build_by_category(spark)
        .writeTo(BY_CATEGORY_TABLE)
        .using("iceberg")
        .partitionedBy(F.months("jour"))
        .createOrReplace()
    )

    print("gold validations dashboards refreshed")


if __name__ == "__main__":
    main()
