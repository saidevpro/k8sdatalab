import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


NESSIE_CATALOG = os.getenv("NESSIE_CATALOG", "nessie")
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "gold")

DAILY_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.validations_daily"
PROFILE_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.validations_hourly_profile"
CALENDAR_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.day_type_calendar"
STOP_AREAS_TABLE = f"{NESSIE_CATALOG}.{SILVER_NAMESPACE}.stop_areas"

CROWDING_FEATURES_TABLE = f"{NESSIE_CATALOG}.{GOLD_NAMESPACE}.crowding_features"


def ensure_namespace(spark: SparkSession) -> None:
    nessie_ref = spark.conf.get(f"spark.sql.catalog.{NESSIE_CATALOG}.ref")
    spark.sql(
        f"CREATE BRANCH IF NOT EXISTS {nessie_ref} IN {NESSIE_CATALOG} FROM main"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {NESSIE_CATALOG}.{GOLD_NAMESPACE}"
    )


def build_crowding_features(spark: SparkSession):
    daily = (
        spark.read.table(DAILY_TABLE)
        .groupBy("jour", "stif_trns", "stif_res", "stif_arret", "id_refa_lda", "libelle_arret")
        .agg(F.sum("nb_vald").alias("daily_total_validations"))
    )
    profile = spark.read.table(PROFILE_TABLE)
    calendar = spark.read.table(CALENDAR_TABLE)
    geo = spark.read.table(STOP_AREAS_TABLE).select(
        F.col("station_id").alias("id_refa_lda"),
        F.when(
            F.col("station_lon").isNotNull() & F.col("station_lat").isNotNull(),
            F.concat(
                F.lit("POINT ("), F.col("station_lon").cast("string"),
                F.lit(" "), F.col("station_lat").cast("string"), F.lit(")"),
            ),
        ).alias("geo_point"),
    )

    return (
        daily.alias("d")
        .join(calendar.alias("c"), F.col("d.jour") == F.col("c.date"), "inner")
        .join(
            profile.alias("p"),
            (F.col("d.stif_trns") == F.col("p.stif_trns"))
            & (F.col("d.stif_res") == F.col("p.stif_res"))
            & (F.col("d.stif_arret") == F.col("p.stif_arret"))
            & (F.col("c.cat_jour") == F.col("p.cat_jour")),
            "inner",
        )
        .join(geo, "id_refa_lda", "left")
        .select(
            F.col("d.id_refa_lda").alias("id_refa_lda"),
            F.col("d.stif_trns").alias("stif_trns"),
            F.col("d.stif_res").alias("stif_res"),
            F.col("d.stif_arret").alias("stif_arret"),
            F.col("d.libelle_arret").alias("libelle_arret"),
            F.col("d.jour").alias("jour"),
            F.year("d.jour").alias("year"),
            F.month("d.jour").alias("month"),
            F.col("c.day_of_week").alias("day_of_week"),
            F.col("c.is_weekend").alias("is_weekend"),
            F.col("c.is_holiday").alias("is_holiday"),
            F.col("c.is_school_holiday").alias("is_school_holiday"),
            F.col("c.cat_jour").alias("cat_jour"),
            F.col("p.hour_start").alias("hour_start"),
            F.col("d.daily_total_validations").alias("daily_total_validations"),
            F.col("p.pct_validations").alias("pct_validations"),
            (F.col("d.daily_total_validations") * F.col("p.pct_validations") / 100).alias(
                "estimated_validations"
            ),
            F.col("geo_point"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_namespace(spark)

    (
        build_crowding_features(spark)
        .writeTo(CROWDING_FEATURES_TABLE)
        .using("iceberg")
        .partitionedBy(F.months("jour"))
        .createOrReplace()
    )

    print("gold crowding_features refreshed")


if __name__ == "__main__":
    main()
